using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using SwiftLogger;
using SwiftLogger.Enums;
using System.Threading.Tasks.Dataflow;

namespace CryptoCandleMetricsProcessor.PostProcessing
{
    public static class DataPostProcessor
    {
        private const int BatchSize = 100000;
        private static readonly object _logLock = new object();

        public static async Task ProcessDataAsync(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            string connectionString = $"Data Source={System.IO.Path.GetFullPath(dbFilePath)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                var productGranularityPairs = await GetProductGranularityPairsAsync(connection, tableName);
                await ThreadSafeLog(logger, LogLevel.Information, $"Found {productGranularityPairs.Count} product-granularity pairs to process.");

                foreach (var pair in productGranularityPairs)
                {
                    await ProcessProductGranularityAsync(connection, tableName, pair.ProductId, pair.Granularity, logger);
                }

                await ThreadSafeLog(logger, LogLevel.Information, "All product-granularity pairs processed.");

                await CheckRemainingNulls(connection, tableName, logger);
            }

            await ThreadSafeLog(logger, LogLevel.Information, "Post-processing completed successfully.");
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
            try
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
                        await ThreadSafeLog(logger, LogLevel.Error, $"Error processing {productId} - {granularity}: {ex.Message}");
                    }
                }
            }
            catch (InvalidOperationException ex)
            {
                await ThreadSafeLog(logger, LogLevel.Error, $"Unable to process {productId} - {granularity}: {ex.Message}");
            }
        }

        private static async Task<long> GetTotalRowCount(SqliteConnection connection, string tableName, string productId, string granularity)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE ProductId = @productId AND Granularity = @granularity";
            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);

            var result = await command.ExecuteScalarAsync();

            if (result is long count)
            {
                return count;
            }
            else if (result is int intCount)
            {
                return intCount;
            }
            else if (result != null)
            {
                return Convert.ToInt64(result);
            }

            throw new InvalidOperationException($"Unable to get row count for {productId} - {granularity}");
        }

        private static async Task ProcessBatchAsync(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, SwiftLogger.SwiftLogger logger)
        {
            try
            {
                await ThreadSafeLog(logger, LogLevel.Information, $"Starting batch processing for {productId} - {granularity} at offset {offset}");

                await BackfillFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, logger);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed backfilling fields for {productId} - {granularity} at offset {offset}");

                await FillWithNeutralValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed filling neutral values for {productId} - {granularity} at offset {offset}");

                await FillBooleanFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed filling boolean fields for {productId} - {granularity} at offset {offset}");

                await FillCategoricalFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed filling categorical fields for {productId} - {granularity} at offset {offset}");

                await BackfillLaggedFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, logger);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed backfilling lagged fields for {productId} - {granularity} at offset {offset}");

                await FillSpecificFieldsInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);
                await ThreadSafeLog(logger, LogLevel.Information, $"Completed filling specific fields for {productId} - {granularity} at offset {offset}");

                await ThreadSafeLog(logger, LogLevel.Information, $"Completed batch processing for {productId} - {granularity} at offset {offset}");
            }
            catch (Exception ex)
            {
                await ThreadSafeLog(logger, LogLevel.Error, $"Error processing batch for {productId} - {granularity} at offset {offset}: {ex.Message}");
                throw;
            }
        }

        private static async Task BackfillFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, SwiftLogger.SwiftLogger logger)
        {
            var fields = new[] { "SMA", "EMA", "ATR", "TEMA", "BB_SMA", "BB_UpperBand", "BB_LowerBand",
                         "MACD", "MACD_Signal", "SuperTrend", "OBV", "RollingMean", "RollingStdDev",
                         "RollingVariance", "RollingSkewness", "RollingKurtosis", "ADL", "ParabolicSar",
                         "PivotPoint", "Resistance1", "Resistance2", "Resistance3", "Support1", "Support2", "Support3",
                         "VWAP", "DynamicSupportLevel", "DynamicResistanceLevel" };

            for (int i = 0; i < fields.Length; i++)
            {
                var field = fields[i];
                var startTime = DateTime.Now;

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

                var endTime = DateTime.Now;
                var duration = (endTime - startTime).TotalSeconds;
                await ThreadSafeLog(logger, LogLevel.Information, $"Backfilled {field} ({i + 1}/{fields.Length}) in {duration:F2} seconds for {productId} - {granularity}");
            }
        }

        private static async Task FillWithNeutralValueInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            var fields = new[] { "RSI", "ADX", "BB_PercentB", "Stoch_K", "Stoch_D", "WilliamsR", "CMF", "CCI" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, fields, 50);
        }

        private static async Task FillWithValueInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, string[] fields, double value)
        {
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
            var fields = new[] { "IsUptrend", "IsBullishCyclePhase" };
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
                { "PriceActionPattern", "Mixed" },
                { "VolatilityRegime", "Medium" },
                { "VolumeProfile", "Normal" }
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
                      AND ({field.Key} IS NULL OR {field.Key} = '')
                      AND Id BETWEEN @offset AND @endOffset";

                command.Parameters.AddWithValue("@defaultValue", field.Value);
                command.Parameters.AddWithValue("@productId", productId);
                command.Parameters.AddWithValue("@granularity", granularity);
                command.Parameters.AddWithValue("@offset", offset);
                command.Parameters.AddWithValue("@endOffset", offset + batchSize - 1);

                await command.ExecuteNonQueryAsync();
            }
        }

        private static async Task BackfillLaggedFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize, SwiftLogger.SwiftLogger logger)
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

            for (int i = 0; i < laggedFields.Length; i++)
            {
                var laggedField = laggedFields[i];
                var startTime = DateTime.Now;

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

                var endTime = DateTime.Now;
                var duration = (endTime - startTime).TotalSeconds;
                await ThreadSafeLog(logger, LogLevel.Information, $"Backfilled {laggedField} ({i + 1}/{laggedFields.Length}) in {duration:F2} seconds for {productId} - {granularity}");
            }
        }

        private static async Task FillSpecificFieldsInBatch(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, long offset, int batchSize)
        {
            await CalculateDayOfWeekInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize);

            var defaultOneFields = new[] { "RelativeVolume" };
            await FillWithValueInBatch(connection, transaction, tableName, productId, granularity, offset, batchSize, defaultOneFields, 1);

            var defaultZeroFields = new[] { "ClosePriceIncrease", "ClosePriceIncreaseStreak", "ClosedHigherThanOpen", "TrendStrength", "TrendDuration", "CompositeSentiment", "CompositeMomentum",
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
            await SetPragmaSettings(connection, logger);

            var columns = await GetTableColumns(connection, tableName);
            var productGranularityPairs = await GetProductGranularityPairsAsync(connection, tableName);

            foreach (var pair in productGranularityPairs)
            {
                await CheckAndBackfillNullsForPair(connection, tableName, pair.ProductId, pair.Granularity, columns, logger);
            }
        }

        private static async Task CheckAndBackfillNullsForPair(SqliteConnection connection, string tableName, string productId, string granularity, List<(string Name, string Type)> columns, SwiftLogger.SwiftLogger logger)
        {
            var command = connection.CreateCommand();
            var nullCheckQueries = new List<string>();

            foreach (var (columnName, columnType) in columns)
            {
                if (columnType.ToUpper() == "TEXT")
                {
                    nullCheckQueries.Add($"SUM(CASE WHEN {columnName} IS NULL OR {columnName} = '' THEN 1 ELSE 0 END) AS {columnName}_null_count");
                }
                else
                {
                    nullCheckQueries.Add($"SUM(CASE WHEN {columnName} IS NULL THEN 1 ELSE 0 END) AS {columnName}_null_count");
                }
            }

            var nullCheckQuery = string.Join(", ", nullCheckQueries);
            command.CommandText = $"SELECT {nullCheckQuery} FROM {tableName} WHERE ProductId = @productId AND Granularity = @granularity;";
            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);

            using var nullCheckReader = await command.ExecuteReaderAsync();
            if (await nullCheckReader.ReadAsync())
            {
                var columnsToBackfill = new List<(string Name, string Type)>();
                for (int i = 0; i < nullCheckReader.FieldCount; i++)
                {
                    var nullCount = nullCheckReader.GetInt64(i);
                    var columnName = columns[i].Name;
                    if (nullCount > 0)
                    {
                        await ThreadSafeLog(logger, LogLevel.Warning, $"Column {columnName} has {nullCount} null or empty values for {productId} - {granularity}.");
                        columnsToBackfill.Add(columns[i]);
                    }
                }

                if (columnsToBackfill.Any())
                {
                    await BackfillRemainingNulls(connection, tableName, productId, granularity, columnsToBackfill, logger);
                }
            }
        }

        private static async Task BackfillRemainingNulls(SqliteConnection connection, string tableName, string productId, string granularity, List<(string Name, string Type)> columnsToBackfill, SwiftLogger.SwiftLogger logger)
        {
            var updateClauses = new List<string>();

            foreach (var (columnName, columnType) in columnsToBackfill)
            {
                if (columnType.ToUpper() == "TEXT")
                {
                    updateClauses.Add($@"
                    {columnName} = COALESCE(
                        (SELECT t2.{columnName}
                        FROM {tableName} t2
                        WHERE t2.ProductId = {tableName}.ProductId
                          AND t2.Granularity = {tableName}.Granularity
                          AND t2.{columnName} IS NOT NULL
                          AND t2.{columnName} != ''
                          AND t2.Id > {tableName}.Id
                        ORDER BY t2.Id
                        LIMIT 1),
                        CASE 
                            WHEN '{columnName}' = 'SentimentCategory' THEN 'Neutral'
                            WHEN '{columnName}' = 'MarketRegime' THEN 'Normal'
                            WHEN '{columnName}' = 'PriceActionPattern' THEN 'Mixed'
                            WHEN '{columnName}' = 'VolatilityRegime' THEN 'Medium'
                            WHEN '{columnName}' = 'VolumeProfile' THEN 'Normal'
                            ELSE ''
                        END
                    )");
                    }
                    else
                    {
                        updateClauses.Add($@"
                    {columnName} = COALESCE(
                        (SELECT t2.{columnName}
                        FROM {tableName} t2
                        WHERE t2.ProductId = {tableName}.ProductId
                          AND t2.Granularity = {tableName}.Granularity
                          AND t2.{columnName} IS NOT NULL
                          AND t2.Id > {tableName}.Id
                        ORDER BY t2.Id
                        LIMIT 1),
                        {columnName}
                    )");
                }
            }

            var updateQuery = $@"
                UPDATE {tableName}
                SET {string.Join(",", updateClauses)}
                WHERE ProductId = @productId
                  AND Granularity = @granularity
                  AND ({string.Join(" OR ", columnsToBackfill.Select(c => c.Type.ToUpper() == "TEXT" ? $"({c.Name} IS NULL OR {c.Name} = '')" : $"{c.Name} IS NULL"))});";

            var command = connection.CreateCommand();
            command.CommandText = updateQuery;
            command.Parameters.AddWithValue("@productId", productId);
            command.Parameters.AddWithValue("@granularity", granularity);

            int rowsAffected = await command.ExecuteNonQueryAsync();
            await ThreadSafeLog(logger, LogLevel.Information, $"BackfillRemainingNulls: Updated {rowsAffected} rows for {productId} - {granularity}.");
        }

        private static async Task SetPragmaSettings(SqliteConnection connection, SwiftLogger.SwiftLogger logger)
        {
            var pragmaSettings = new[]
            {
                "PRAGMA journal_mode = WAL;",
                "PRAGMA synchronous = NORMAL;",
                "PRAGMA cache_size = -32000;", // 32MB cache
                "PRAGMA temp_store = MEMORY;",
                "PRAGMA mmap_size = 10000000000;", // 10GB (adjust based on available RAM)
                "PRAGMA page_size = 32768;" // 32KB pages
            };

            foreach (var setting in pragmaSettings)
            {
                using var cmd = new SqliteCommand(setting, connection);
                await cmd.ExecuteNonQueryAsync();
                await ThreadSafeLog(logger, LogLevel.Information, $"Executed: {setting}");
            }
        }

        private static async Task<List<(string Name, string Type)>> GetTableColumns(SqliteConnection connection, string tableName)
        {
            var columns = new List<(string Name, string Type)>();
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT name, type FROM pragma_table_info('{tableName}');";

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add((reader.GetString(0), reader.GetString(1).ToUpper()));
            }

            return columns;
        }

        private static async Task LogProgress(long processedRows, long totalRows, string productId, string granularity, SwiftLogger.SwiftLogger logger)
        {
            long actualProcessedRows = Math.Min(processedRows, totalRows);
            double percentage = Math.Min((double)actualProcessedRows / totalRows * 100, 100);
            await ThreadSafeLog(logger, LogLevel.Information, $"Processed {actualProcessedRows:N0} out of {totalRows:N0} rows ({percentage:F2}% complete) for {productId} - {granularity}");
        }

        private static Task ThreadSafeLog(SwiftLogger.SwiftLogger logger, LogLevel level, string message)
        {
            return Task.Run(() =>
            {
                lock (_logLock)
                {
                    logger.Log(level, message).Wait();
                }
            });
        }
    }
}