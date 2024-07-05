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
                Console.WriteLine("5. Post-process data");
                Console.WriteLine("6. Export database to CSV");
                Console.WriteLine("7. Exit");

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
                        await logger.Log(LogLevel.Information, "Starting data post-processing.");
                        await DataPostProcessor.ProcessDataAsync(dbFilePath, tableName, logger);
                        await logger.Log(LogLevel.Information, "Data post-processing completed successfully.");
                        break;

                    case "6":
                        await logger.Log(LogLevel.Information, "Exporting database to CSV.");
                        await CsvExporter.ExportDatabaseToCsvAsync(dbFilePath, logger);
                        await logger.Log(LogLevel.Information, "CSV data exported successfully.");
                        break;

                    case "7":
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
    }
}
