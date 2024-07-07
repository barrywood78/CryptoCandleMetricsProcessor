using CryptoCandleMetricsProcessor.Analysis;
using CryptoCandleMetricsProcessor.Database;
using CryptoCandleMetricsProcessor.Models;
using CryptoCandleMetricsProcessor.Importers;
using CryptoCandleMetricsProcessor.Exporters;
using SwiftLogger;
using SwiftLogger.Configs;
using SwiftLogger.Enums;
using CryptoCandleMetricsProcessor.PostProcessing;
using System;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;

namespace CryptoCandleMetricsProcessor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Load configuration
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Read configuration values
            string importFolderPath = config["ImportFolderPath"] ?? throw new InvalidOperationException("ImportFolderPath is not set in the configuration.");
            string exportFolderPath = config["ExportFolderPath"] ?? throw new InvalidOperationException("ExportFolderPath is not set in the configuration.");
            string dbFilePath = config["DatabaseFilePath"] ?? throw new InvalidOperationException("DatabaseFilePath is not set in the configuration.");

            DateTime startTime = DateTime.Now;
            string timestamp = startTime.ToString("yyyyMMdd_HHmmss");

            var consoleConfig = new ConsoleLoggerConfig()
                .SetColorForLogLevel(LogLevel.Error, ConsoleColor.Red)
                .SetColorForLogLevel(LogLevel.Warning, ConsoleColor.Yellow)
                .SetMinimumLogLevel(LogLevel.Information);

            var fileConfig = new FileLoggerConfig()
                .SetFilePath($"CryptoCandleMetricsProcessor-{timestamp}.txt")
                .EnableSeparationByDate();

            var logger = new LoggerConfigBuilder()
                .LogTo.Console(consoleConfig)
                .LogTo.File(fileConfig)
                .Build();

            await logger.Log(LogLevel.Information, $"Application started at: {startTime}");

            // Get the field definitions from the separate class
            var fields = Fields.GetFields();

            // Define the mappings between CSV columns and database fields
            var mappings = new List<CsvToDbMapping>
            {
                new CsvToDbMapping { CsvColumnIndex = 0, DbFieldName = "ProductId" },
                new CsvToDbMapping { CsvColumnIndex = 1, DbFieldName = "Granularity" },
                new CsvToDbMapping { CsvColumnIndex = 2, DbFieldName = "StartUnix" },
                new CsvToDbMapping { CsvColumnIndex = 3, DbFieldName = "StartDate" },
                new CsvToDbMapping { CsvColumnIndex = 4, DbFieldName = "Low" },
                new CsvToDbMapping { CsvColumnIndex = 5, DbFieldName = "High" },
                new CsvToDbMapping { CsvColumnIndex = 6, DbFieldName = "Open" },
                new CsvToDbMapping { CsvColumnIndex = 7, DbFieldName = "Close" },
                new CsvToDbMapping { CsvColumnIndex = 8, DbFieldName = "Volume" }
            };

            // Define the table name
            string tableName = "Candles";

            bool exit = false;
            while (!exit)
            {
                Console.WriteLine("Select an option:");
                Console.WriteLine("1. Delete existing database");
                Console.WriteLine("2. Create database with table");
                Console.WriteLine("3. Import CSV files into database");
                Console.WriteLine("4. Calculate technical analysis indicators");
                Console.WriteLine("5. Create DB Indexes");
                Console.WriteLine("6. Post-process data");
                Console.WriteLine("7. Export database to CSV");
                Console.WriteLine("8. Perform all operations (1-7)");
                Console.WriteLine("9. Exit");

                var input = Console.ReadLine();
                bool operationComplete = false;

                switch (input)
                {
                    case "1":
                        operationComplete = await DeleteDatabase(dbFilePath, logger);
                        break;

                    case "2":
                        operationComplete = await CreateDatabase(dbFilePath, tableName, fields, logger);
                        break;

                    case "3":
                        operationComplete = await ImportCsvFiles(importFolderPath, dbFilePath, tableName, mappings, logger);
                        break;

                    case "4":
                        operationComplete = await CalculateIndicators(dbFilePath, tableName, logger);
                        break;

                    case "5":
                        operationComplete = await CreateDatabaseIndexes(dbFilePath, tableName, logger);
                        break;

                    case "6":
                        operationComplete = await PostProcessData(dbFilePath, tableName, logger);
                        break;

                    case "7":
                        operationComplete = await ExportToCsv(dbFilePath, exportFolderPath, logger);
                        break;

                    case "8":
                        operationComplete = await PerformAllOperations(dbFilePath, tableName, fields, importFolderPath, exportFolderPath, mappings, logger);
                        break;

                    case "9":
                        exit = true;
                        break;

                    default:
                        Console.WriteLine("Invalid option. Please try again.");
                        break;
                }

                if (!exit && operationComplete)
                {
                    Console.WriteLine("\nProcess completed. Press any key to return to the menu.");
                    Console.ReadKey();
                    Console.Clear();
                }
            }

            var endTime = DateTime.Now;
            await logger.Log(LogLevel.Information, $"Application completed at: {endTime}");

            // Calculate the duration
            var duration = endTime - startTime;
            await logger.Log(LogLevel.Information, $"Total time taken: {duration.Hours} hours, {duration.Minutes} minutes, {duration.Seconds} seconds");
        }

        private static async Task<bool> DeleteDatabase(string dbFilePath, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Attempting to delete existing database.");
            if (File.Exists(dbFilePath))
            {
                for (int attempts = 0; attempts < 5; attempts++)
                {
                    try
                    {
                        File.Delete(dbFilePath);
                        await logger.Log(LogLevel.Information, "Existing database deleted.");
                        return true;
                    }
                    catch (IOException ex)
                    {
                        await logger.Log(LogLevel.Warning, $"Attempt {attempts + 1} to delete database failed: {ex.Message}");
                        if (attempts < 4)
                        {
                            await logger.Log(LogLevel.Information, "Waiting before next attempt...");
                            await Task.Delay(1000); // Wait for 1 second before trying again
                        }
                    }
                }
                await logger.Log(LogLevel.Error, "Failed to delete database after multiple attempts. The file may be locked by another process.");
                return false;
            }
            else
            {
                await logger.Log(LogLevel.Warning, "Database file not found.");
                return true;
            }
        }

        private static async Task<bool> CreateDatabase(string dbFilePath, string tableName, List<FieldDefinition> fields, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Creating database with table.");
            try
            {
                await DatabaseCreator.CreateDatabaseWithTableAsync(dbFilePath, tableName, fields);
                await logger.Log(LogLevel.Information, "Database created with table.");
                return true;
            }
            catch (Exception ex)
            {
                await logger.Log(LogLevel.Error, $"Error creating database: {ex.Message}");
                return false;
            }
            finally
            {
                // Ensure that all connections are closed
                SqliteConnection.ClearAllPools();
            }
        }

        private static async Task<bool> ImportCsvFiles(string importFolderPath, string dbFilePath, string tableName, List<CsvToDbMapping> mappings, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Starting CSV import process.");
            var csvFilePaths = Directory.GetFiles(importFolderPath, "*.csv");
            int totalRowsImported = 0;

            foreach (var csvFilePath in csvFilePaths)
            {
                int rowsImported = CsvImporter.ImportCsvToDatabase(csvFilePath, dbFilePath, tableName, mappings, true);
                totalRowsImported += rowsImported;
                await logger.Log(LogLevel.Information, $"CSV data from {Path.GetFileName(csvFilePath)} imported successfully. Rows imported: {rowsImported}");
            }

            await logger.Log(LogLevel.Information, $"CSV import process completed. Total rows imported: {totalRowsImported}");
            return true;
        }

        private static async Task<bool> CalculateIndicators(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Calculating technical analysis indicators.");
            await TechnicalAnalysis.CalculateIndicatorsAsync(dbFilePath, tableName, logger);
            await logger.Log(LogLevel.Information, "Indicators calculated successfully.");
            return true;
        }

        private static async Task<bool> PostProcessData(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Starting data post-processing.");
            await DataPostProcessor.ProcessDataAsync(dbFilePath, tableName, logger);
            await logger.Log(LogLevel.Information, "Data post-processing completed successfully.");
            return true;
        }

        private static async Task<bool> ExportToCsv(string dbFilePath, string exportFolderPath, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Exporting database to CSV.");
            await CsvExporter.ExportDatabaseToCsvAsync(dbFilePath, logger, exportFolderPath);
            await logger.Log(LogLevel.Information, "CSV data exported successfully.");
            return true;
        }

        private static async Task<bool> CreateDatabaseIndexes(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
            await logger.Log(LogLevel.Information, "Creating database indexes.");
            using (var connection = new SqliteConnection($"Data Source={dbFilePath}"))
            {
                await connection.OpenAsync();

                var indexCommands = new[]
                {
                    $"CREATE INDEX IF NOT EXISTS idx_product_granularity ON {tableName} (ProductId, Granularity)",
                    $"CREATE INDEX IF NOT EXISTS idx_id ON {tableName} (Id)",
                    $"CREATE INDEX IF NOT EXISTS idx_start_unix ON {tableName} (StartUnix)",
                    $"CREATE INDEX IF NOT EXISTS idx_start_date ON {tableName} (StartDate)",
                    $"CREATE INDEX IF NOT EXISTS idx_close ON {tableName} (Close)",
                    $"CREATE INDEX IF NOT EXISTS idx_volume ON {tableName} (Volume)",
                    $"CREATE INDEX IF NOT EXISTS idx_buy_score ON {tableName} (BuyScore)",
                    $"CREATE INDEX IF NOT EXISTS idx_rsi ON {tableName} (RSI)",
                    $"CREATE INDEX IF NOT EXISTS idx_stoch_k ON {tableName} (Stoch_K)",
                    $"CREATE INDEX IF NOT EXISTS idx_stoch_d ON {tableName} (Stoch_D)",
                    $"CREATE INDEX IF NOT EXISTS idx_adx ON {tableName} (ADX)",
                    $"CREATE INDEX IF NOT EXISTS idx_bb_percentb ON {tableName} (BB_PercentB)",
                    $"CREATE INDEX IF NOT EXISTS idx_cmf ON {tableName} (CMF)",
                    $"CREATE INDEX IF NOT EXISTS idx_macd_histogram ON {tableName} (MACD_Histogram)",
                    $"CREATE INDEX IF NOT EXISTS idx_adl ON {tableName} (ADL)",
                    $"CREATE INDEX IF NOT EXISTS idx_ema ON {tableName} (EMA)",
                    $"CREATE INDEX IF NOT EXISTS idx_sma ON {tableName} (SMA)",
                    $"CREATE INDEX IF NOT EXISTS idx_macd ON {tableName} (MACD)",
                    $"CREATE INDEX IF NOT EXISTS idx_macd_signal ON {tableName} (MACD_Signal)",
                    $"CREATE INDEX IF NOT EXISTS idx_lagged_macd_1 ON {tableName} (Lagged_MACD_1)",
                    $"CREATE INDEX IF NOT EXISTS idx_lagged_close_1 ON {tableName} (Lagged_Close_1)",
                    $"CREATE INDEX IF NOT EXISTS idx_main_query ON {tableName} (ProductId, Granularity, RSI, Stoch_K, Stoch_D, ADX, BB_PercentB, CMF, MACD_Histogram, ADL, EMA, SMA, MACD, MACD_Signal)",
                    "ANALYZE"
                };

                foreach (var cmd in indexCommands)
                {
                    using var command = new SqliteCommand(cmd, connection);
                    await command.ExecuteNonQueryAsync();
                    await logger.Log(LogLevel.Information, $"Executed: {cmd}");
                }
            }
            await logger.Log(LogLevel.Information, "Database indexes created successfully.");
            return true;
        }

        private static async Task<bool> PerformAllOperations(string dbFilePath, string tableName, List<FieldDefinition> fields, string importFolderPath, string exportFolderPath, List<CsvToDbMapping> mappings, SwiftLogger.SwiftLogger logger)
        {
            if (!await DeleteDatabase(dbFilePath, logger)) return false;
            if (!await CreateDatabase(dbFilePath, tableName, fields, logger)) return false;
            if (!await ImportCsvFiles(importFolderPath, dbFilePath, tableName, mappings, logger)) return false;
            if (!await CalculateIndicators(dbFilePath, tableName, logger)) return false;
            if (!await CreateDatabaseIndexes(dbFilePath, tableName, logger)) return false;
            if (!await PostProcessData(dbFilePath, tableName, logger)) return false;
            if (!await ExportToCsv(dbFilePath, exportFolderPath, logger)) return false;
            return true;
        }
    }
}