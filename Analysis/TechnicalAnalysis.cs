using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;
using CryptoCandleMetricsProcessor.Analysis.Indicators;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis
{
    public static class TechnicalAnalysis
    {
        /// <summary>
        /// Calculates technical analysis indicators for each product and granularity combination in the database.
        /// </summary>
        /// <param name="dbFilePath">The path to the SQLite database file.</param>
        /// <param name="tableName">The name of the table containing the candle data.</param>
        public static void CalculateIndicators(string dbFilePath, string tableName)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";
            var groupedCandles = new Dictionary<(string ProductId, string Granularity), List<Quote>>();

            // Open a connection to the SQLite database
            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                // Select all relevant columns ordered by ProductId, Granularity, and StartUnix
                string selectQuery = $"SELECT ProductId, Granularity, StartUnix, StartDate, Open, High, Low, Close, Volume FROM {tableName} ORDER BY ProductId, Granularity, StartUnix";

                using (var command = new SqliteCommand(selectQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    // Read and group candle data by ProductId and Granularity
                    while (reader.Read())
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

                // Iterate through each product and granularity combination
                foreach (var group in groupedCandles)
                {
                    var productId = group.Key.ProductId;
                    var granularity = group.Key.Granularity;
                    var candles = group.Value;

                    // Define custom periods for each indicator based on granularity
                    var periods = GetPeriodsForGranularity(granularity);

                    // Dictionary to store indicator names and their corresponding calculation methods
                    var indicators = new Dictionary<string, Action<SqliteConnection, SqliteTransaction, string, string, string, List<Quote>, Dictionary<string, int>>>
                    {
                        {"PriceUp", (conn, trans, table, prod, gran, c, p) => PriceUpIndicator.Calculate(conn, trans, table, prod, gran, c)},
                        {"PriceUpStreak", (conn, trans, table, prod, gran, c, p) => PriceUpStreakIndicator.Calculate(conn, trans, table, prod, gran, c)},
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
                        {"CompositeMarketSentiment", (conn, trans, table, prod, gran, c, p) => CompositeMarketSentimentIndicator.Calculate(conn, trans, table, prod, gran, c)},
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
                        {"VWAP", (conn, trans, table, prod, gran, c, p) => VWAPIndicator.Calculate(conn, trans, table, prod, gran, c)}
                    };

                    // If you have any specific parameters to be passed to the indicators, ensure to add them to the GetPeriodsForGranularity method and pass accordingly.


                    int totalIndicators = indicators.Count;
                    int currentIndicator = 0;

                    // Calculate each indicator with its own transaction
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
                                Console.WriteLine($"[{currentIndicator}/{totalIndicators}] Calculated {indicatorName} for {productId} - {granularity}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error calculating {indicatorName} for {productId} - {granularity}: {ex.Message}");
                                transaction.Rollback();
                            }
                        }
                    }
                }

                // Calculate CandlePatternIndicator separately with its own transaction
                foreach (var group in groupedCandles)
                {
                    var productId = group.Key.ProductId;
                    var granularity = group.Key.Granularity;
                    var candles = group.Value;

                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            CandlePatternIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                            transaction.Commit();
                            Console.WriteLine($"CandlePattern calculated for {productId} - {granularity}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error calculating CandlePattern for {productId} - {granularity}: {ex.Message}");
                            transaction.Rollback();
                        }
                    }
                }


                // Create indexes and run ANALYZE
                CreateIndexesAndAnalyze(dbFilePath, tableName);
                Console.WriteLine("Indexes created and ANALYZE run successfully.");

                // Calculate BuySignals for all product/granularity combinations
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
                            Console.WriteLine($"BuySignal calculated for {productId} - {granularity}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error calculating BuySignal for {productId} - {granularity}: {ex.Message}");
                            transaction.Rollback();
                        }
                    }
                }
            }
        }

        // Returns a dictionary of custom periods for each indicator based on granularity
        private static Dictionary<string, int> GetPeriodsForGranularity(string granularity)
        {
            // Customize periods for each indicator based on granularity
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

        // Creates indexes on the specified columns and runs the ANALYZE command
        static void CreateIndexesAndAnalyze(string dbFilePath, string tableName)
        {
            using (var connection = new SqliteConnection($"Data Source={dbFilePath}"))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        -- Index on ProductId and Granularity (composite index)
                        CREATE INDEX IF NOT EXISTS idx_product_granularity ON {tableName}(ProductId, Granularity);

                        -- Index on StartDate
                        CREATE INDEX IF NOT EXISTS idx_start_date ON {tableName}(StartDate);

                        -- Index on BuyScore (for percentile calculations)
                        CREATE INDEX IF NOT EXISTS idx_buy_score ON {tableName}(BuyScore);

                        -- Indexes on individual columns used in the BuyScore calculation
                        CREATE INDEX IF NOT EXISTS idx_rsi ON {tableName}(RSI);
                        CREATE INDEX IF NOT EXISTS idx_stoch_k ON {tableName}(Stoch_K);
                        CREATE INDEX IF NOT EXISTS idx_stoch_d ON {tableName}(Stoch_D);
                        CREATE INDEX IF NOT EXISTS idx_adx ON {tableName}(ADX);
                        CREATE INDEX IF NOT EXISTS idx_bb_percentb ON {tableName}(BB_PercentB);
                        CREATE INDEX IF NOT EXISTS idx_cmf ON {tableName}(CMF);
                        CREATE INDEX IF NOT EXISTS idx_macd_histogram ON {tableName}(MACD_Histogram);
                        CREATE INDEX IF NOT EXISTS idx_adl ON {tableName}(ADL);
                        CREATE INDEX IF NOT EXISTS idx_ema ON {tableName}(EMA);
                        CREATE INDEX IF NOT EXISTS idx_sma ON {tableName}(SMA);
                        CREATE INDEX IF NOT EXISTS idx_macd ON {tableName}(MACD);
                        CREATE INDEX IF NOT EXISTS idx_macd_signal ON {tableName}(MACD_Signal);

                        -- Indexes on lagged columns
                        CREATE INDEX IF NOT EXISTS idx_lagged_macd_1 ON {tableName}(Lagged_MACD_1);
                        CREATE INDEX IF NOT EXISTS idx_lagged_close_1 ON {tableName}(Lagged_Close_1);

                        -- Composite index for the main query in CalculateBuyScoresInDatabase
                        CREATE INDEX IF NOT EXISTS idx_main_query ON {tableName}(ProductId, Granularity, RSI, Stoch_K, Stoch_D, ADX, BB_PercentB, CMF, MACD_Histogram, ADL, EMA, SMA, MACD, MACD_Signal);

                        -- Run ANALYZE
                        ANALYZE;
                    ";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}