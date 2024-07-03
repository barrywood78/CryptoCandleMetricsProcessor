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

                var tasks = productGranularityPairs.Select(pair => ProcessPairAsync(connectionString, tableName, pair.ProductId, pair.Granularity, logger)).ToArray();
                await Task.WhenAll(tasks);

                await CheckRemainingNulls(connectionString, tableName, logger);
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

        private static async Task ProcessPairAsync(string connectionString, string tableName, string productId, string granularity, SwiftLogger.SwiftLogger logger)
        {
            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var tasks = new List<Task>
                        {
                            BackfillFields(connection, tableName, productId, granularity, new[]
                            {
                                "SMA", "EMA", "ATR", "TEMA", "BB_SMA", "BB_UpperBand", "BB_LowerBand",
                                "MACD", "MACD_Signal", "SuperTrend", "OBV", "RollingMean", "RollingStdDev",
                                "RollingVariance", "RollingSkewness", "RollingKurtosis", "ADL", "ParabolicSar",
                                "PivotPoint", "Resistance1", "Resistance2", "Resistance3", "Support1", "Support2", "Support3",
                                "FibRetracement_23_6", "FibRetracement_38_2", "FibRetracement_50", "FibRetracement_61_8", "FibRetracement_78_6",
                                "VWAP", "DynamicSupportLevel", "DynamicResistanceLevel"
                            }),

                            FillWithNeutralValue(connection, tableName, productId, granularity, new[]
                            {
                                "RSI", "ADX", "BB_PercentB", "Stoch_K", "Stoch_D", "WilliamsR", "CMF", "CCI"
                            }, 50),

                            FillWithValue(connection, tableName, productId, granularity, new[]
                            {
                                "PriceUpStreak", "BuyScore", "BB_ZScore", "BB_Width", "MACD_Histogram",
                                "PriceChangePercent", "VolumeChangePercent", "ATRPercent", "RSIChange",
                                "MACDHistogramSlope", "DistanceToNearestSupport", "DistanceToNearestResistance",
                                "DistanceToDynamicSupport", "DistanceToDynamicResistance", "ADLChange"
                            }, 0),

                            FillBooleanFields(connection, tableName, productId, granularity, new[]
                            {
                                "PriceUp", "IsUptrend", "IsBullishCyclePhase"
                            }, false),

                            FillCategoricalFields(connection, tableName, productId, granularity),

                            BackfillLaggedFields(connection, tableName, productId, granularity, new[]
                            {
                                "Lagged_Close", "Lagged_RSI", "Lagged_Return", "Lagged_EMA", "Lagged_ATR",
                                "Lagged_MACD", "Lagged_BollingerUpper", "Lagged_BollingerLower",
                                "Lagged_BollingerPercentB", "Lagged_StochK", "Lagged_StochD"
                            }),

                            FillSpecificFields(connection, tableName, productId, granularity)
                        };

                        await Task.WhenAll(tasks);

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
        }

        private static async Task BackfillFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields)
        {
            var tasks = fields.Select(field =>
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

                return command.ExecuteNonQueryAsync();
            });

            await Task.WhenAll(tasks);
        }

        private static async Task FillWithNeutralValue(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, double neutralValue)
        {
            await FillWithValue(connection, tableName, productId, granularity, fields, neutralValue);
        }

        private static async Task FillWithValue(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, double value)
        {
            var tasks = fields.Select(field =>
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = @value
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field} IS NULL;";

                command.Parameters.AddWithValue("@value", value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                return command.ExecuteNonQueryAsync();
            });

            await Task.WhenAll(tasks);
        }

        private static async Task FillBooleanFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] fields, bool defaultValue)
        {
            var tasks = fields.Select(field =>
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = @defaultValue
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field} IS NULL;";

                command.Parameters.AddWithValue("@defaultValue", defaultValue ? 1 : 0);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                return command.ExecuteNonQueryAsync();
            });

            await Task.WhenAll(tasks);
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

            var tasks = categoricalFields.Select(field =>
            {
                var command = connection.CreateCommand();
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field.Key} = @defaultValue
                    WHERE ProductId = @productId AND Granularity = @granularity AND {field.Key} IS NULL;";

                command.Parameters.AddWithValue("@defaultValue", field.Value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);

                return command.ExecuteNonQueryAsync();
            });

            await Task.WhenAll(tasks);
        }

        private static async Task BackfillLaggedFields(SqliteConnection connection, string tableName, string productId, string granularity, string[] baseFields)
        {
            var tasks = baseFields.SelectMany(baseField =>
                Enumerable.Range(1, 3).Select(i =>
                {
                    var field = $"{baseField}_{i}";
                    return BackfillFields(connection, tableName, productId, granularity, new[] { field });
                })
            );

            await Task.WhenAll(tasks);
        }

        private static async Task FillSpecificFields(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            var tasks = new List<Task>
            {
                CalculateDayOfWeek(connection, tableName, productId, granularity),

                FillWithValue(connection, tableName, productId, granularity, new[] { "RelativeVolume", "VolumeProfile" }, 1),

                FillWithValue(connection, tableName, productId, granularity, new[] { "TrendStrength", "TrendDuration", "CompositeSentiment", "CompositeMomentum" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "MACDCrossover", "EMACrossover" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "CycleDominantPeriod", "CyclePhase" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "FractalDimension", "HurstExponent", "MarketEfficiencyRatio" }, 0.5),

                FillWithValue(connection, tableName, productId, granularity, new[] { "MarketVolatility", "OrderFlowImbalance" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "RSIDivergence", "MACDDivergence" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "RSIDivergenceStrength" }, 0),

                FillWithValue(connection, tableName, productId, granularity, new[] { "HistoricalVolatility", "ROC_5", "ROC_10" }, 0)
            };

            await Task.WhenAll(tasks);
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

        private static async Task CheckRemainingNulls(string connectionString, string tableName, SwiftLogger.SwiftLogger logger)
        {
            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

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

                var tasks = numericColumns.Select(async column =>
                {
                    command = connection.CreateCommand();
                    command.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE {column} IS NULL;";
                    long nullCount = (long)(await command.ExecuteScalarAsync() ?? 0L);


                    if (nullCount > 0)
                    {
                        await logger.Log(LogLevel.Warning, $"Column {column} still has {nullCount} null values.");

                        // Backfill remaining nulls based on the next non-null value
                        await BackfillRemainingNulls(connection, tableName, column, logger);
                    }
                });

                await Task.WhenAll(tasks);
            }
        }

        private static async Task BackfillRemainingNulls(SqliteConnection connection, string tableName, string column, SwiftLogger.SwiftLogger logger)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"
                WITH next_non_null AS (
                    SELECT RowId, LEAD({column}) OVER (ORDER BY RowId) as NextNonNullValue
                    FROM {tableName}
                )
                UPDATE {tableName}
                SET {column} = (SELECT NextNonNullValue FROM next_non_null WHERE {tableName}.RowId = next_non_null.RowId)
                WHERE {column} IS NULL;";

            int rowsAffected = await command.ExecuteNonQueryAsync();
            await logger.Log(LogLevel.Information, $"BackfillRemainingNulls: Updated {rowsAffected} rows for {column} with the next non-null value.");
        }
    }
}
