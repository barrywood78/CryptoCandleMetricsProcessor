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
        private const int BatchSize = 100000;

        public static async Task ProcessDataAsync(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            string connectionString = $"Data Source={System.IO.Path.GetFullPath(dbFilePath)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                await CreateIndexesAndAnalyze(connection, tableName, logger);

                var productGranularityPairs = await GetProductGranularityPairsAsync(connection, tableName);
                await logger.Log(LogLevel.Information, $"Found {productGranularityPairs.Count} product-granularity pairs to process.");

                foreach (var pair in productGranularityPairs)
                {
                    await ProcessProductGranularityAsync(connection, tableName, pair.ProductId, pair.Granularity, logger);
                }

                await logger.Log(LogLevel.Information, "All product-granularity pairs processed.");

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

        private static async Task ProcessProductGranularityAsync(SqliteConnection connection, string tableName, string productId, string granularity, SwiftLogger.SwiftLogger logger)
        {
            long totalRows = await GetTotalRowCount(connection, tableName, productId, granularity);

            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    for (long offset = 0; offset < totalRows; offset += BatchSize)
                    {
                        await ProcessBatchAsync(connection, transaction, tableName, productId, granularity, offset, BatchSize, logger);
                        await LogProgress(offset + BatchSize, totalRows, productId, granularity, logger);
                    }
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    await logger.Log(LogLevel.Error, $"Error processing {productId} - {granularity}: {ex.Message}");
                }
            }
        }

        private static async Task<long> GetTotalRowCount(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE ProductId = @productId AND Granularity = @granularity";
            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);
            return (long)await command.ExecuteScalarAsync();
        }

        private static async Task ProcessBatchAsync(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, SwiftLogger.SwiftLogger logger)
        {
            try
            {
                await BackfillFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await FillWithNeutralValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await FillBooleanFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await FillCategoricalFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await BackfillLaggedFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await FillSpecificFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
            }
            catch (Exception ex)
            {
                await logger.Log(LogLevel.Error, $"Error processing batch for {productId} - {granularity} at offset {offset}: {ex.Message}");
                throw;
            }
        }

        private static async Task BackfillFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var fields = new[] { "SMA", "EMA", "ATR", "TEMA", "BB_SMA", "BB_UpperBand", "BB_LowerBand",
                                 "MACD", "MACD_Signal", "SuperTrend", "OBV", "RollingMean", "RollingStdDev",
                                 "RollingVariance", "RollingSkewness", "RollingKurtosis", "ADL", "ParabolicSar",
                                 "PivotPoint", "Resistance1", "Resistance2", "Resistance3", "Support1", "Support2", "Support3",
                                 "FibRetracement_23_6", "FibRetracement_38_2", "FibRetracement_50", "FibRetracement_61_8", "FibRetracement_78_6",
                                 "VWAP", "DynamicSupportLevel", "DynamicResistanceLevel" };

            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = (
                        SELECT {field}
                        FROM {tableName} AS t2
                        WHERE t2.ProductId = @productId
                          AND t2.Granularity = @granularity
                          AND t2.{field} IS NOT NULL
                          AND t2.Id <= {tableName}.Id
                          AND t2.Id BETWEEN @offset AND @endOffset
                        ORDER BY t2.Id DESC
                        LIMIT 1
                    )
                    WHERE ProductId = @productId
                      AND Granularity = @granularity
                      AND {field} IS NULL
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task FillWithNeutralValueInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var fields = new[] { "RSI", "ADX", "BB_PercentB", "Stoch_K", "Stoch_D", "WilliamsR", "CMF", "CCI" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, fields, 50);
        }

        private static async Task FillWithValueInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, string[] fields = null, double value = 0)
        {
            fields = fields ?? new[] { "PriceUp", "PriceUpStreak", "BuyScore", "BB_ZScore", "BB_Width", "MACD_Histogram",
                                       "PriceChangePercent", "VolumeChangePercent", "ATRPercent", "RSIChange",
                                       "MACDHistogramSlope", "DistanceToNearestSupport", "DistanceToNearestResistance",
                                       "DistanceToDynamicSupport", "DistanceToDynamicResistance", "ADLChange" };

            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = @value
                    WHERE ProductId = @productId
                      AND Granularity = @granularity
                      AND {field} IS NULL
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@value", value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task FillBooleanFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var fields = new[] { "PriceUp", "IsUptrend", "IsBullishCyclePhase" };
            foreach (var field in fields)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field} = 0
                    WHERE ProductId = @productId
                      AND Granularity = @granularity
                      AND {field} IS NULL
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task FillCategoricalFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
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
                command.Transaction = transaction;
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {field.Key} = @defaultValue
                    WHERE ProductId = @productId
                      AND Granularity = @granularity
                      AND {field.Key} IS NULL
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@defaultValue", field.Value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task BackfillLaggedFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var laggedFields = new[]
            {
                "Lagged_Close_1", "Lagged_Close_2", "Lagged_Close_3",
                "Lagged_RSI_1", "Lagged_RSI_2", "Lagged_RSI_3",
                "Lagged_Return_1", "Lagged_Return_2", "Lagged_Return_3",
                "Lagged_EMA_1", "Lagged_EMA_2", "Lagged_EMA_3",
                "Lagged_ATR_1", "Lagged_ATR_2", "Lagged_ATR_3",
                "Lagged_MACD_1", "Lagged_MACD_2", "Lagged_MACD_3",
                "Lagged_BollingerUpper_1", "Lagged_BollingerUpper_2", "Lagged_BollingerUpper_3",
                "Lagged_BollingerLower_1", "Lagged_BollingerLower_2", "Lagged_BollingerLower_3",
                "Lagged_BollingerPercentB_1", "Lagged_BollingerPercentB_2", "Lagged_BollingerPercentB_3",
                "Lagged_StochK_1", "Lagged_StochK_2", "Lagged_StochK_3",
                "Lagged_StochD_1", "Lagged_StochD_2", "Lagged_StochD_3"
            };

            foreach (var laggedField in laggedFields)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $@"
                    UPDATE {tableName}
                    SET {laggedField} = (
                        SELECT {laggedField}
                        FROM {tableName} AS t2
                        WHERE t2.ProductId = @productId
                          AND t2.Granularity = @granularity
                          AND t2.Id < {tableName}.Id
                          AND t2.Id BETWEEN @offset AND @endOffset
                          AND t2.{laggedField} IS NOT NULL
                        ORDER BY t2.Id DESC
                        LIMIT 1
                    )
                    WHERE ProductId = @productId
                      AND Granularity = @granularity
                      AND {laggedField} IS NULL
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task FillSpecificFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            await CalculateDayOfWeekInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);

            var defaultOneFields = new[] { "RelativeVolume", "VolumeProfile" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, defaultOneFields, 1);

            var defaultZeroFields = new[] { "PriceUp", "PriceUpStreak", "TrendStrength", "TrendDuration", "CompositeSentiment", "CompositeMomentum",
                                            "MACDCrossover", "EMACrossover", "CycleDominantPeriod", "CyclePhase",
                                            "MarketVolatility", "OrderFlowImbalance", "RSIDivergence", "MACDDivergence",
                                            "RSIDivergenceStrength", "HistoricalVolatility", "ROC_5", "ROC_10" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, defaultZeroFields, 0);

            var defaultHalfFields = new[] { "FractalDimension", "HurstExponent", "MarketEfficiencyRatio" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, defaultHalfFields, 0.5);
        }

        private static async Task CalculateDayOfWeekInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"
                UPDATE {tableName}
                SET DayOfWeek = CAST(strftime('%w', StartDate) AS INTEGER) + 1
                WHERE ProductId = @productId 
                  AND Granularity = @granularity
                  AND Id BETWEEN @offset AND @endOffset;";

            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);
            command.Parameters.AddWithValue("@offset", offset);
            command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

            await command.ExecuteNonQueryAsync();
        }

        private static async Task CheckRemainingNulls(SqliteConnection connection, string tableName, SwiftLogger.SwiftLogger logger)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"
                SELECT name 
                FROM pragma_table_info('{tableName}')
                WHERE type IN ('INTEGER', 'REAL', 'NUMERIC');";

            var numericColumns = new List<string>();
            using (var columnReader = await command.ExecuteReaderAsync())
            {
                while (await columnReader.ReadAsync())
                {
                    numericColumns.Add(columnReader.GetString(0));
                }
            }

            var nullCheckQuery = string.Join(", ", numericColumns.Select(c => $"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null_count"));
            command.CommandText = $"SELECT {nullCheckQuery} FROM {tableName};";

            using var nullCheckReader = await command.ExecuteReaderAsync();
            if (await nullCheckReader.ReadAsync())
            {
                for (int i = 0; i < nullCheckReader.FieldCount; i++)
                {
                    var nullCount = nullCheckReader.GetInt64(i);
                    var columnName = numericColumns[i];
                    if (nullCount > 0)
                    {
                        await logger.Log(LogLevel.Warning, $"Column {columnName} still has {nullCount} null values.");
                        await BackfillRemainingNulls(connection, tableName, columnName, logger);
                    }
                }
            }
        }

        private static async Task BackfillRemainingNulls(SqliteConnection connection, string tableName, string column, SwiftLogger.SwiftLogger logger)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"
                UPDATE {tableName} AS t1
                SET {column} = (
                    SELECT t2.{column}
                    FROM {tableName} AS t2
                    WHERE t2.ProductId = t1.ProductId
                      AND t2.Granularity = t1.Granularity
                      AND t2.{column} IS NOT NULL
                      AND t2.Id > t1.Id
                    ORDER BY t2.Id
                    LIMIT 1
                )
                WHERE {column} IS NULL;";

            int rowsAffected = await command.ExecuteNonQueryAsync();
            await logger.Log(LogLevel.Information, $"BackfillRemainingNulls: Updated {rowsAffected} rows for {column} with the nearest non-null value.");
        }

        private static async Task CreateIndexesAndAnalyze(SqliteConnection connection, string tableName, SwiftLogger.SwiftLogger logger)
        {
            var indexCommands = new[]
            {
                $"CREATE INDEX IF NOT EXISTS idx_product_granularity ON {tableName} (ProductId, Granularity)",
                $"CREATE INDEX IF NOT EXISTS idx_id ON {tableName} (Id)",
                $"CREATE INDEX IF NOT EXISTS idx_start_unix ON {tableName} (StartUnix)",
                $"CREATE INDEX IF NOT EXISTS idx_product_granularity_id ON {tableName} (ProductId, Granularity, Id)",
                $"CREATE INDEX IF NOT EXISTS idx_close ON {tableName} (Close)",
                $"CREATE INDEX IF NOT EXISTS idx_volume ON {tableName} (Volume)",
                "ANALYZE"
            };

            foreach (var cmd in indexCommands)
            {
                using var command = connection.CreateCommand();
                command.CommandText = cmd;
                await command.ExecuteNonQueryAsync();
                await logger.Log(LogLevel.Information, $"Executed: {cmd}");
            }

            await logger.Log(LogLevel.Information, "Indexes created and database analyzed.");
        }

        private static async Task LogProgress(long processedRows, long totalRows, string productId, string granularity, SwiftLogger.SwiftLogger logger)
        {
            // Ensure processedRows doesn't exceed totalRows
            long actualProcessedRows = Math.Min(processedRows, totalRows);

            // Calculate percentage, capped at 100%
            double percentage = Math.Min((double)actualProcessedRows / totalRows * 100, 100);

            await logger.Log(LogLevel.Information, $"Processed {actualProcessedRows:N0} out of {totalRows:N0} rows ({percentage:F2}% complete) for {productId} - {granularity}");
        }
    }
}