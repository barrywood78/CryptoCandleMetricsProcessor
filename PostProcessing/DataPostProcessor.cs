using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using SwiftLogger;
using SwiftLogger.Enums;

namespace CryptoCandleMetricsProcessor.PostProcessing
{
    public static class DataPostProcessor
    {
        public static async Task ProcessDataAsync(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            string connectionString = $"Data Source={System.IO.Path.GetFullPath(dbFilePath)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                var productGranularityPairs = await GetProductGranularityPairsAsync(connection, tableName);
                await logger.Log(LogLevel.Information, $"Found {productGranularityPairs.Count} product-granularity pairs to process.");

                foreach (var pair in productGranularityPairs)
                {
                    await ProcessPairAsync(connection, tableName, pair.ProductId, pair.Granularity, logger);
                }

                await CheckRemainingNulls(connection, tableName, logger);
            }

            await logger.Log(LogLevel.Information, "Post-processing completed successfully.");
        }

        private static async Task<List<(string ProductId, string Granularity)>> GetProductGranularityPairsAsync(SqliteConnection connection, string tableName)
        {
            var pairs = new List<(string, string)>();
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT DISTINCT ProductId, Granularity FROM {tableName}";

            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    pairs.Add((reader.GetString(0), reader.GetString(1)));
                }
            }

            return pairs;
        }

        private static async Task ProcessPairAsync(SqliteConnection connection, string tableName, string productId, string granularity, SwiftLogger.SwiftLogger logger)
        {
            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    // Backfill for trend-based indicators and most other numerical fields
                    var backfillFields = new[]
                    {
                        "SMA", "EMA", "ATR", "TEMA", "BB_SMA", "BB_UpperBand", "BB_LowerBand",
                        "MACD", "MACD_Signal", "SuperTrend", "OBV", "RollingMean", "RollingStdDev",
                        "RollingVariance", "RollingSkewness", "RollingKurtosis", "ADL", "ParabolicSar",
                        "PivotPoint", "Resistance1", "Resistance2", "Resistance3", "Support1", "Support2", "Support3",
                        "FibRetracement_23_6", "FibRetracement_38_2", "FibRetracement_50", "FibRetracement_61_8", "FibRetracement_78_6",
                        "VWAP", "DynamicSupportLevel", "DynamicResistanceLevel"
                    };
                    await BackfillFields(connection, tableName, productId, granularity, backfillFields);

                    // Fill with neutral value for oscillators and bounded indicators
                    var neutralValueFields = new[]
                    {
                        "RSI", "ADX", "BB_PercentB", "Stoch_K", "Stoch_D", "WilliamsR", "CMF", "CCI"
                    };
                    await FillWithNeutralValue(connection, tableName, productId, granularity, neutralValueFields, 50);

                    // Fill with zero for difference-based indicators
                    var zeroFillFields = new[]
                    {
                        "PriceUpStreak", "BuyScore", "BB_ZScore", "BB_Width", "MACD_Histogram",
                        "PriceChangePercent", "VolumeChangePercent", "ATRPercent", "RSIChange",
                        "MACDHistogramSlope", "DistanceToNearestSupport", "DistanceToNearestResistance",
                        "DistanceToDynamicSupport", "DistanceToDynamicResistance", "ADLChange"  
                    };
                    await FillWithValue(connection, tableName, productId, granularity, zeroFillFields, 0);

                    // Handle boolean fields
                    var booleanFields = new[] { "PriceUp", "IsUptrend", "IsBullishCyclePhase" };
                    await FillBooleanFields(connection, tableName, productId, granularity, booleanFields, false);

                    // Handle categorical fields
                    await FillCategoricalFields(connection, tableName, productId, granularity);

                    // Handle lagged fields
                    var laggedFields = new[]
                    {
                        "Lagged_Close", "Lagged_RSI", "Lagged_Return", "Lagged_EMA", "Lagged_ATR",
                        "Lagged_MACD", "Lagged_BollingerUpper", "Lagged_BollingerLower",
                        "Lagged_BollingerPercentB", "Lagged_StochK", "Lagged_StochD"
                    };
                    await BackfillLaggedFields(connection, tableName, productId, granularity, laggedFields);

                    // Handle specific fields
                    await FillSpecificFields(connection, tableName, productId, granularity);

                    transaction.Commit();
                    await logger.Log(LogLevel.Information, $"Processed data for {productId} - {granularity}");
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    await logger.Log(LogLevel.Error, $"Error processing data for {productId} - {granularity}: {ex.Message}");
                }
            }
        }

        private static async Task BackfillFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields)
        {
            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    WITH first_non_null AS (
                        SELECT MIN(RowId) as FirstNonNullRowId, {field} as FirstNonNullValue
                        FROM {tableName}
                        WHERE ProductId = @productId AND Granularity = @granularity AND {field} IS NOT NULL
                    )
                    UPDATE {tableName}
                    SET {field} = (SELECT FirstNonNullValue FROM first_non_null)
                    WHERE ProductId = @productId 
                      AND Granularity = @granularity 
                      AND {field} IS NULL 
                      AND RowId < (SELECT FirstNonNullRowId FROM first_non_null);";

                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                int rowsAffected = await command.ExecuteNonQueryAsync();
                //Console.WriteLine($"Backfill: Updated {rowsAffected} rows for {field}");
            }
        }

        private static async Task FillWithNeutralValue(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, double neutralValue)
        {
            await FillWithValue(connection, tableName, productId, granularity, fields, neutralValue);
        }

        private static async Task FillWithValue(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, double value)
        {
            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = @value
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field} IS NULL;";

                command.Parameters.AddWithValue("@value", value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                int rowsAffected = await command.ExecuteNonQueryAsync();
                //Console.WriteLine($"FillWithValue: Updated {rowsAffected} rows for {field} with value {value}");
            }
        }

        private static async Task FillBooleanFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, bool defaultValue)
        {
            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = @defaultValue
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field} IS NULL;";

                command.Parameters.AddWithValue("@defaultValue", defaultValue ? 1 : 0);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                int rowsAffected = await command.ExecuteNonQueryAsync();
                //Console.WriteLine($"FillBooleanFields: Updated {rowsAffected} rows for {field} with value {defaultValue}");
            }
        }

        private static async Task FillCategoricalFields(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            var categoricalFields = new Dictionary<string, string>
            {
                { "SentimentCategory", "Neutral" },
                { "MarketRegime", "Normal" },
                { "PriceActionPattern", "NoPattern" },
                { "VolatilityRegime", "Normal" }
            };

            foreach (var field in categoricalFields)
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field.Key} = @defaultValue
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field.Key} IS NULL;";

                command.Parameters.AddWithValue("@defaultValue", field.Value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                int rowsAffected = await command.ExecuteNonQueryAsync();
                //Console.WriteLine($"FillCategoricalFields: Updated {rowsAffected} rows for {field.Key} with value {field.Value}");
            }
        }

        private static async Task BackfillLaggedFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] baseFields)
        {
            foreach (var baseField in baseFields)
            {
                for (int i = 1; i <= 3; i++)
                {
                    var field = $"{baseField}_{i}";
                    await BackfillFields(connection, tableName, productId, granularity, new[] { field });
                }
            }
        }

        private static async Task FillSpecificFields(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            // Fill DayOfWeek with actual calculated values
            await CalculateDayOfWeek(connection, tableName, productId, granularity);

            // Fill RelativeVolume and VolumeProfile with 1 (neutral value)
            await FillWithValue(connection, tableName, productId, granularity, new[] { "RelativeVolume", "VolumeProfile" }, 1);

            // Fill TrendStrength, TrendDuration, CompositeSentiment, CompositeMomentum with 0
            await FillWithValue(connection, tableName, productId, granularity, new[] { "TrendStrength", "TrendDuration", "CompositeSentiment", "CompositeMomentum" }, 0);

            // Fill MACDCrossover and EMACrossover with 0 (no crossover)
            await FillWithValue(connection, tableName, productId, granularity, new[] { "MACDCrossover", "EMACrossover" }, 0);

            // Fill CycleDominantPeriod and CyclePhase with default values
            await FillWithValue(connection, tableName, productId, granularity, new[] { "CycleDominantPeriod", "CyclePhase" }, 0);

            // Fill FractalDimension, HurstExponent, MarketEfficiencyRatio with neutral values
            await FillWithValue(connection, tableName, productId, granularity, new[] { "FractalDimension", "HurstExponent", "MarketEfficiencyRatio" }, 0.5);

            // Fill MarketVolatility and OrderFlowImbalance with 0
            await FillWithValue(connection, tableName, productId, granularity, new[] { "MarketVolatility", "OrderFlowImbalance" }, 0);

            // Fill RSIDivergence and MACDDivergence with 0 (no divergence)
            await FillWithValue(connection, tableName, productId, granularity, new[] { "RSIDivergence", "MACDDivergence" }, 0);

            // Fill RSIDivergenceStrength with 0
            await FillWithValue(connection, tableName, productId, granularity, new[] { "RSIDivergenceStrength" }, 0);

            // Fill HistoricalVolatility, ROC_5, ROC_10 with 0
            await FillWithValue(connection, tableName, productId, granularity, new[] { "HistoricalVolatility", "ROC_5", "ROC_10" }, 0);
        }

        private static async Task CalculateDayOfWeek(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"
                UPDATE {tableName}
                SET DayOfWeek = CAST(strftime('%w', StartDate) AS INTEGER) + 1
                WHERE ProductId = @productId AND Granularity = @granularity;";

            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);

            await command.ExecuteNonQueryAsync();
        }

        private static async Task CheckRemainingNulls(SqliteConnection connection, string tableName, SwiftLogger.SwiftLogger logger)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"
                SELECT name FROM pragma_table_info('{tableName}')
                WHERE type LIKE 'INTEGER' OR type LIKE 'REAL' OR type LIKE 'NUMERIC';";

            var numericColumns = new List<string>();
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    numericColumns.Add(reader.GetString(0));
                }
            }

            foreach (var column in numericColumns)
            {
                command = connection.CreateCommand();
                command.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE {column} IS NULL;";
                long nullCount = (long)await command.ExecuteScalarAsync();

                if (nullCount > 0)
                {
                    await logger.Log(LogLevel.Warning, $"Column {column} still has {nullCount} null values.");

                    // Optional: Log a sample of rows with null values for this column
                    command = connection.CreateCommand();
                    command.CommandText = $@"
                        SELECT ProductId, Granularity, StartUnix, StartDate
                        FROM {tableName}
                        WHERE {column} IS NULL
                        LIMIT 5;";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string sampleRow = $"ProductId: {reader.GetString(0)}, " +
                                               $"Granularity: {reader.GetString(1)}, " +
                                               $"StartUnix: {reader.GetInt64(2)}, " +
                                               $"StartDate: {reader.GetDateTime(3)}";
                            await logger.Log(LogLevel.Warning, $"Sample row with null {column}: {sampleRow}");
                        }
                    }
                }
            }
        }
    }
}