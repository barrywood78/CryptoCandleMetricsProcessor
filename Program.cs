using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CryptoCandleMetricsProcessor.Analysis;
using CryptoCandleMetricsProcessor.Database;
using CryptoCandleMetricsProcessor.Models;
using CryptoCandleMetricsProcessor.Importers;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Exporters;
using System.Reflection.Metadata;
using System.Text;
using CryptoCandleMetricsProcessor.Utilities;

namespace CryptoCandleMetricsProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime startTime = DateTime.Now;
            string timestamp = startTime.ToString("yyyyMMdd_HHmmss");
            string logFilePath = $"consoleLog_{timestamp}.txt";

            using (StreamWriter writer = new StreamWriter(logFilePath))
            {
                // Create a multi-writer to write to both console and file
                var multiWriter = new MultiTextWriter(Console.Out, writer);
                Console.SetOut(multiWriter);

                Console.WriteLine($"Application started at: {startTime}");

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
                    Console.WriteLine($"CSV data from {csvFilePath} imported successfully.");
                }

                // Calculate technical analysis indicators
                TechnicalAnalysis.CalculateIndicators(dbFilePath, tableName);
                Console.WriteLine("Indicators calculated successfully.");

                // Export the database to CSV
                CsvExporter.ExportDatabaseToCsv(dbFilePath);
                Console.WriteLine("CSV data exported successfully.");

                var endTime = DateTime.Now;
                Console.WriteLine($"Application completed at: {endTime}");

                // Calculate the duration
                var duration = endTime - startTime;
                Console.WriteLine($"Total time taken: {duration.Hours} hours, {duration.Minutes} minutes, {duration.Seconds} seconds");
            }

            // Reset console output to standard output
            Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
        }
    }

    
}
