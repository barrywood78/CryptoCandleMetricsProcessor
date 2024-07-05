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

namespace CryptoCandleMetricsProcessor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            DateTime startTime = DateTime.Now;
            string timestamp = startTime.ToString("yyyyMMdd_HHmmss");

            var consoleConfig = new ConsoleLoggerConfig()
                .SetColorForLogLevel(LogLevel.Error, ConsoleColor.Red)
                .SetColorForLogLevel(LogLevel.Warning, ConsoleColor.Yellow)
                .SetMinimumLogLevel(LogLevel.Information);

            var fileConfig = new FileLoggerConfig()
                .SetFilePath($"MetricsProcessorLog-{timestamp}.txt")
                .EnableSeparationByDate();

            var logger = new LoggerConfigBuilder()
                .LogTo.Console(consoleConfig)
                .LogTo.File(fileConfig)
                .Build();

            await logger.Log(LogLevel.Information, $"Application started at: {startTime}");

            // Path to the folder containing CSV files
            string folderPath = "C:\\Users\\DELL PC\\Desktop\\Candle Data\\";

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

            // Define the database file path and table name
            string dbFilePath = "candles_data.sqlite";
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
                Console.WriteLine("8. Exit");

                var input = Console.ReadLine();
                switch (input)
                {
                    case "1":
                        await logger.Log(LogLevel.Information, "Attempting to delete existing database.");
                        if (File.Exists(dbFilePath))
                        {
                            File.Delete(dbFilePath);
                            await logger.Log(LogLevel.Information, "Existing database deleted.");
                        }
                        else
                        {
                            await logger.Log(LogLevel.Warning, "Database file not found.");
                        }
                        break;

                    case "2":
                        await logger.Log(LogLevel.Information, "Creating database with table.");
                        DatabaseCreator.CreateDatabaseWithTable(dbFilePath, tableName, fields);
                        await logger.Log(LogLevel.Information, "Database created with table.");
                        break;

                    case "3":
                        await logger.Log(LogLevel.Information, "Starting CSV import process.");
                        var csvFilePaths = Directory.GetFiles(folderPath, "*.csv");
                        foreach (var csvFilePath in csvFilePaths)
                        {
                            CsvImporter.ImportCsvToDatabase(csvFilePath, dbFilePath, tableName, mappings, true);
                            await logger.Log(LogLevel.Information, $"CSV data from {csvFilePath} imported successfully.");
                        }
                        break;

                    case "4":
                        await logger.Log(LogLevel.Information, "Calculating technical analysis indicators.");
                        await TechnicalAnalysis.CalculateIndicatorsAsync(dbFilePath, tableName, logger);
                        await logger.Log(LogLevel.Information, "Indicators calculated successfully.");
                        break;

                    case "5":
                        await logger.Log(LogLevel.Information, "Creating database indexes.");
                        await CreateDatabaseIndexes(dbFilePath, tableName, logger);
                        await logger.Log(LogLevel.Information, "Database indexes created successfully.");
                        break;

                    case "6":
                        await logger.Log(LogLevel.Information, "Starting data post-processing.");
                        await DataPostProcessor.ProcessDataAsync(dbFilePath, tableName, logger);
                        await logger.Log(LogLevel.Information, "Data post-processing completed successfully.");
                        break;

                    case "7":
                        await logger.Log(LogLevel.Information, "Exporting database to CSV.");
                        await CsvExporter.ExportDatabaseToCsvAsync(dbFilePath, logger);
                        await logger.Log(LogLevel.Information, "CSV data exported successfully.");
                        break;

                    case "8":
                        exit = true;
                        break;

                    default:
                        Console.WriteLine("Invalid option. Please try again.");
                        break;
                }

                if (!exit)
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

        private static async Task CreateDatabaseIndexes(string dbFilePath, string tableName, SwiftLogger.SwiftLogger logger)
        {
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
        }
    }
}