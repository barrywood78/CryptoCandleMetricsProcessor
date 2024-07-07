using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Analysis.Indicators;
using Skender.Stock.Indicators;
using SwiftLogger.Enums;

namespace CryptoCandleMetricsProcessor.Analysis
{
    public static class TechnicalAnalysis
    {
        public static async Task CalculateIndicatorsAsync(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger, string granularity = "", string productId = "")
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";
            var groupedCandles = new Dictionary<(string ProductId, string Granularity), List<Quote>>();

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                // Apply PRAGMA settings
                using (var pragmaCommand = connection.CreateCommand())
                {
                    pragmaCommand.CommandText = @"
                        PRAGMA journal_mode = WAL;
                        PRAGMA synchronous = NORMAL;
                        PRAGMA cache_size = -10000;
                        PRAGMA temp_store = MEMORY;";
                    await pragmaCommand.ExecuteNonQueryAsync();
                }

                // Select and group candle data
                string selectQuery = $@"
                    SELECT ProductId, Granularity, StartUnix, StartDate, Open, High, Low, Close, Volume 
                    FROM {tableName} 
                    WHERE 1=1";  // This is to make it easier to append conditions

                if (!string.IsNullOrEmpty(granularity))
                {
                    selectQuery += " AND Granularity = @Granularity";
                }

                if (!string.IsNullOrEmpty(productId))
                {
                    selectQuery += " AND ProductId = @ProductId";
                }

                selectQuery += " ORDER BY ProductId, Granularity, StartUnix";

                using (var command = new SqliteCommand(selectQuery, connection))
                {
                    if (!string.IsNullOrEmpty(granularity))
                    {
                        command.Parameters.AddWithValue("@Granularity", granularity);
                    }

                    if (!string.IsNullOrEmpty(productId))
                    {
                        command.Parameters.AddWithValue("@ProductId", productId);
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var key = (ProductId: reader.GetString(0), Granularity: reader.GetString(1));
                            if (!groupedCandles.ContainsKey(key))
                            {
                                groupedCandles[key] = new List<Quote>();
                            }

                            groupedCandles[key].Add(new Quote
                            {
                                Date = reader.GetDateTime(3),
                                Open = reader.GetDecimal(4),
                                High = reader.GetDecimal(5),
                                Low = reader.GetDecimal(6),
                                Close = reader.GetDecimal(7),
                                Volume = reader.GetDecimal(8)
                            });
                        }
                    }
                }

                // Process indicators
                foreach (var group in groupedCandles)
                {
                    var groupProductId = group.Key.ProductId;
                    var groupGranularity = group.Key.Granularity;
                    var candles = group.Value;
                    var periods = GetPeriodsForGranularity(groupGranularity);

                    await ProcessIndicatorsAsync(connection, tableName, groupProductId, groupGranularity, candles, periods, logger);
                }

                // Process CandlePatternIndicator
                await ProcessCandlePatternsAsync(connection, tableName, groupedCandles, logger);

                // Create indexes and run ANALYZE
                //await CreateIndexesAndAnalyzeAsync(dbFilePath, tableName);
                //await logger.Log(LogLevel.Information, "Indexes created and ANALYZE run successfully.");

                // Calculate BuySignals
                await CalculateBuySignalsAsync(connection, tableName, groupedCandles, logger);
            }
        }

        private static async Task ProcessIndicatorsAsync(SqliteConnection connection, string tableName, string productId, string granularity, List<Quote> candles, Dictionary<string, int> periods, SwiftLogger.SwiftLogger logger)
        {
            var indicators = GetIndicators();
            int totalIndicators = indicators.Count;
            int currentIndicator = 0;

            foreach (var indicatorPair in indicators)
            {
                currentIndicator++;
                string indicatorName = indicatorPair.Key;
                var indicator = indicatorPair.Value;

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        indicator(connection, transaction, tableName, productId, granularity, candles, periods);
                        transaction.Commit();
                        await logger.Log(LogLevel.Information, $"[{currentIndicator}/{totalIndicators}] Calculated {indicatorName} for {productId} - {granularity}");
                    }
                    catch (Exception ex)
                    {
                        await logger.Log(LogLevel.Error, $"Error calculating {indicatorName} for {productId} - {granularity}: {ex.Message}");
                        transaction.Rollback();
                    }
                }
            }
        }

        private static async Task ProcessCandlePatternsAsync(SqliteConnection connection, string tableName, Dictionary<(string ProductId, string Granularity), List<Quote>> groupedCandles, SwiftLogger.SwiftLogger logger)
        {
            foreach (var group in groupedCandles)
            {
                var productId = group.Key.ProductId;
                var granularity = group.Key.Granularity;
                var candles = group.Value;

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        await CandlePatternIndicator.CalculateAsync(connection, transaction, tableName, productId, granularity, candles, logger);
                        await logger.Log(LogLevel.Information, $"CandlePattern calculated for {productId} - {granularity}");
                    }
                    catch (Exception ex)
                    {
                        await logger.Log(LogLevel.Error, $"Error calculating CandlePattern for {productId} - {granularity}: {ex.Message}");
                    }
                }
            }
        }

        //private static async Task CreateIndexesAndAnalyzeAsync(string dbFilePath, string tableName)
        //{
        //    using (var connection = new SqliteConnection($"Data Source={dbFilePath}"))
        //    {
        //        await connection.OpenAsync();
        //        using (var command = connection.CreateCommand())
        //        {
        //            command.CommandText = $@"
        //                -- Index on ProductId and Granularity (composite index)
        //                CREATE INDEX IF NOT EXISTS idx_product_granularity ON {tableName}(ProductId, Granularity);

        //                -- Index on StartDate
        //                CREATE INDEX IF NOT EXISTS idx_start_date ON {tableName}(StartDate);

        //                -- Index on BuyScore (for percentile calculations)
        //                CREATE INDEX IF NOT EXISTS idx_buy_score ON {tableName}(BuyScore);

        //                -- Indexes on individual columns used in the BuyScore calculation
        //                CREATE INDEX IF NOT EXISTS idx_rsi ON {tableName}(RSI);
        //                CREATE INDEX IF NOT EXISTS idx_stoch_k ON {tableName}(Stoch_K);
        //                CREATE INDEX IF NOT EXISTS idx_stoch_d ON {tableName}(Stoch_D);
        //                CREATE INDEX IF NOT EXISTS idx_adx ON {tableName}(ADX);
        //                CREATE INDEX IF NOT EXISTS idx_bb_percentb ON {tableName}(BB_PercentB);
        //                CREATE INDEX IF NOT EXISTS idx_cmf ON {tableName}(CMF);
        //                CREATE INDEX IF NOT EXISTS idx_macd_histogram ON {tableName}(MACD_Histogram);
        //                CREATE INDEX IF NOT EXISTS idx_adl ON {tableName}(ADL);
        //                CREATE INDEX IF NOT EXISTS idx_ema ON {tableName}(EMA);
        //                CREATE INDEX IF NOT EXISTS idx_sma ON {tableName}(SMA);
        //                CREATE INDEX IF NOT EXISTS idx_macd ON {tableName}(MACD);
        //                CREATE INDEX IF NOT EXISTS idx_macd_signal ON {tableName}(MACD_Signal);

        //                -- Indexes on lagged columns
        //                CREATE INDEX IF NOT EXISTS idx_lagged_macd_1 ON {tableName}(Lagged_MACD_1);
        //                CREATE INDEX IF NOT EXISTS idx_lagged_close_1 ON {tableName}(Lagged_Close_1);

        //                -- Composite index for the main query in CalculateBuyScoresInDatabase
        //                CREATE INDEX IF NOT EXISTS idx_main_query ON {tableName}(ProductId, Granularity, RSI, Stoch_K, Stoch_D, ADX, BB_PercentB, CMF, MACD_Histogram, ADL, EMA, SMA, MACD, MACD_Signal);

        //                -- Run ANALYZE
        //                ANALYZE;
        //            ";
        //            await command.ExecuteNonQueryAsync();
        //        }
        //    }
        //}

        private static async Task CalculateBuySignalsAsync(SqliteConnection connection, string tableName, Dictionary<(string ProductId, string Granularity), List<Quote>> groupedCandles, SwiftLogger.SwiftLogger logger)
        {
            foreach (var group in groupedCandles)
            {
                var productId = group.Key.ProductId;
                var granularity = group.Key.Granularity;
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        BuySignalIndicator.Calculate(connection, transaction, tableName, productId, granularity);
                        transaction.Commit();
                        await logger.Log(LogLevel.Information, $"BuySignal calculated for {productId} - {granularity}");
                    }
                    catch (Exception ex)
                    {
                        await logger.Log(LogLevel.Error, $"Error calculating BuySignal for {productId} - {granularity}: {ex.Message}");
                        transaction.Rollback();
                    }
                }
            }
        }

        private static Dictionary<string, int> GetPeriodsForGranularity(string granularity)
        {
            return granularity switch
            {
                "FIVE_MINUTE" => new Dictionary<string, int>
                {
                    { "SMA", 9 },
                    { "EMA", 9 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 9 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "FIFTEEN_MINUTE" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "ONE_HOUR" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "ONE_DAY" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                _ => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                }
            };
        }

        private static Dictionary<string, Action<SqliteConnection, SqliteTransaction, string, string, string, List<Quote>, Dictionary<string, int>>> GetIndicators()
        {
            return new Dictionary<string, Action<SqliteConnection, SqliteTransaction, string, string, string, List<Quote>, Dictionary<string, int>>>
            {
                {"ClosePriceIncrease", (conn, trans, table, prod, gran, c, p) => ClosePriceIncreaseIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"ClosePriceIncreaseStreak", (conn, trans, table, prod, gran, c, p) => ClosePriceIncreaseStreakIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"ClosedHigherThanOpen", (conn, trans, table, prod, gran, c, p) => ClosedHigherThanOpenIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"SMA", (conn, trans, table, prod, gran, c, p) => SmaIndicator.Calculate(conn, trans, table, prod, gran, c, p["SMA"])},
                {"EMA", (conn, trans, table, prod, gran, c, p) => EmaIndicator.Calculate(conn, trans, table, prod, gran, c, p["EMA"])},
                {"ATR", (conn, trans, table, prod, gran, c, p) => AtrIndicator.Calculate(conn, trans, table, prod, gran, c, p["ATR"])},
                {"RSI", (conn, trans, table, prod, gran, c, p) => RsiIndicator.Calculate(conn, trans, table, prod, gran, c, p["RSI"])},
                {"ADX", (conn, trans, table, prod, gran, c, p) => AdxIndicator.Calculate(conn, trans, table, prod, gran, c, p["ADX"])},
                {"TEMA", (conn, trans, table, prod, gran, c, p) => TemaIndicator.Calculate(conn, trans, table, prod, gran, c, p["TEMA"])},
                {"BollingerBands", (conn, trans, table, prod, gran, c, p) => BollingerBandsIndicator.Calculate(conn, trans, table, prod, gran, c, p["BollingerBands"])},
                {"Stochastic", (conn, trans, table, prod, gran, c, p) => StochasticIndicator.Calculate(conn, trans, table, prod, gran, c, p["StochasticOscillator"])},
                {"MACD", (conn, trans, table, prod, gran, c, p) => MacdIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"Supertrend", (conn, trans, table, prod, gran, c, p) => SupertrendIndicator.Calculate(conn, trans, table, prod, gran, c, p["Supertrend"])},
                {"WilliamsR", (conn, trans, table, prod, gran, c, p) => WilliamsRIndicator.Calculate(conn, trans, table, prod, gran, c, p["WilliamsR"])},
                {"OBV", (conn, trans, table, prod, gran, c, p) => ObvIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"StatisticalIndicators", (conn, trans, table, prod, gran, c, p) => StatisticalIndicators.Calculate(conn, trans, table, prod, gran, c, p["SMA"])},
                {"ADLine", (conn, trans, table, prod, gran, c, p) => AdLineIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"CMF", (conn, trans, table, prod, gran, c, p) => CmfIndicator.Calculate(conn, trans, table, prod, gran, c, p["CMF"])},
                {"CCI", (conn, trans, table, prod, gran, c, p) => CciIndicator.Calculate(conn, trans, table, prod, gran, c, p["CCI"])},
                {"ParabolicSAR", (conn, trans, table, prod, gran, c, p) => ParabolicSarIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"RollingPivotPoints", (conn, trans, table, prod, gran, c, p) => RollingPivotPointsIndicator.Calculate(conn, trans, table, prod, gran, c, (int)PeriodSize.Day, 10, (int)PivotPointType.Standard)},
                {"FibonacciRetracement", (conn, trans, table, prod, gran, c, p) => FibonacciRetracementIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"LaggedFeatures", (conn, trans, table, prod, gran, c, p) => LaggedFeaturesIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"ADLChange", (conn, trans, table, prod, gran, c, p) => ADLChangeIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"ATRPercent", (conn, trans, table, prod, gran, c, p) => ATRPercentIndicator.Calculate(conn, trans, table, prod, gran, c, p["ATR"])},
                {"CompositeMomentum", (conn, trans, table, prod, gran, c, p) => CompositeMomentumIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"CrossoverFeatures", (conn, trans, table, prod, gran, c, p) => CrossoverFeaturesIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"CyclicalPatterns", (conn, trans, table, prod, gran, c, p) => CyclicalPatternsIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"DayOfWeek", (conn, trans, table, prod, gran, c, p) => DayOfWeekIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"DistanceToSupportResistance", (conn, trans, table, prod, gran, c, p) => DistanceToSupportResistanceIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"DynamicSupportResistance", (conn, trans, table, prod, gran, c, p) => DynamicSupportResistanceIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"FractalDimension", (conn, trans, table, prod, gran, c, p) => FractalDimensionIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"HistoricalVolatility", (conn, trans, table, prod, gran, c, p) => HistoricalVolatilityIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"HurstExponent", (conn, trans, table, prod, gran, c, p) => HurstExponent.Calculate(conn, trans, table, prod, gran, c)},
                {"MACDHistogramSlope", (conn, trans, table, prod, gran, c, p) => MACDHistogramSlopeIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"MarketEfficiencyRatio", (conn, trans, table, prod, gran, c, p) => MarketEfficiencyRatio.Calculate(conn, trans, table, prod, gran, c)},
                {"MarketRegime", (conn, trans, table, prod, gran, c, p) => MarketRegimeIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"OrderFlowImbalance", (conn, trans, table, prod, gran, c, p) => OrderFlowImbalanceIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"OscillatorDivergences", (conn, trans, table, prod, gran, c, p) => OscillatorDivergencesIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"PriceActionClassification", (conn, trans, table, prod, gran, c, p) => PriceActionClassification.Calculate(conn, trans, table, prod, gran, c)},
                {"PriceChangePercent", (conn, trans, table, prod, gran, c, p) => PriceChangePercentIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"PricePositionInRange", (conn, trans, table, prod, gran, c, p) => PricePositionInRangeIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"RelativeVolumeProfile", (conn, trans, table, prod, gran, c, p) => RelativeVolumeProfile.Calculate(conn, trans, table, prod, gran, c)},
                {"ROCIndicator", (conn, trans, table, prod, gran, c, p) => ROCIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"RSIChange", (conn, trans, table, prod, gran, c, p) => RSIChangeIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"RSIDivergenceStrength", (conn, trans, table, prod, gran, c, p) => RSIDivergenceStrengthIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"TrendStrength", (conn, trans, table, prod, gran, c, p) => TrendStrengthIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"VolatilityRegime", (conn, trans, table, prod, gran, c, p) => VolatilityRegime.Calculate(conn, trans, table, prod, gran, c)},
                {"VolumeChangePercent", (conn, trans, table, prod, gran, c, p) => VolumeChangePercentIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"VWAP", (conn, trans, table, prod, gran, c, p) => VWAPIndicator.Calculate(conn, trans, table, prod, gran, c)},
                {"CompositeMarketSentiment", (conn, trans, table, prod, gran, c, p) => CompositeMarketSentimentIndicator.Calculate(conn, trans, table, prod, gran, c)}
            };
        }
    }
}