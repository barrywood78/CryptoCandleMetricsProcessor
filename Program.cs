using CryptoCandleMetricsProcessor.Analysis;
using CryptoCandleMetricsProcessor.Database;
using CryptoCandleMetricsProcessor.Models;
using CryptoCandleMetricsProcessor.Importers;
using CryptoCandleMetricsProcessor.Exporters;
using SwiftLogger;
using SwiftLogger.Configs;
using SwiftLogger.Enums;
using CryptoCandleMetricsProcessor.PostProcessing;

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
                .SetColorForLogLevel(LogLevel.Warning, ConsoleColor.Green)
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

            // Delete the database file if it already exists
            if (File.Exists(dbFilePath))
            {
                File.Delete(dbFilePath);
            }

            // Create the database with the specified table and fields
            DatabaseCreator.CreateDatabaseWithTable(dbFilePath, tableName, fields);

            // Get all CSV file paths from the specified folder
            var csvFilePaths = Directory.GetFiles(folderPath, "*.csv");

            // Import each CSV file into the database
            foreach (var csvFilePath in csvFilePaths)
            {
                CsvImporter.ImportCsvToDatabase(csvFilePath, dbFilePath, tableName, mappings, true);
                await logger.Log(LogLevel.Information, $"CSV data from {csvFilePath} imported successfully.");
            }

            // Calculate technical analysis indicators
            await TechnicalAnalysis.CalculateIndicatorsAsync(dbFilePath, tableName, logger);
            await logger.Log(LogLevel.Information, "Indicators calculated successfully.");

            // Post-process data to handle null values
            await DataPostProcessor.ProcessDataAsync(dbFilePath, tableName, logger);
            await logger.Log(LogLevel.Information, "Data post-processing completed successfully.");

            // Export the database to CSV
            CsvExporter.ExportDatabaseToCsv(dbFilePath);
            await logger.Log(LogLevel.Information, "CSV data exported successfully.");

            var endTime = DateTime.Now;
            await logger.Log(LogLevel.Information, $"Application completed at: {endTime}");

            // Calculate the duration
            var duration = endTime - startTime;
            await logger.Log(LogLevel.Information, $"Total time taken: {duration.Hours} hours, {duration.Minutes} minutes, {duration.Seconds} seconds");
        }


    }


}
