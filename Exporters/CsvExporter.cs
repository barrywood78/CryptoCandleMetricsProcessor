﻿using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;

namespace CryptoCandleMetricsProcessor.Exporters
{
    public static class CsvExporter
    {
        /// <summary>
        /// Exports data from the specified SQLite database to CSV files.
        /// </summary>
        /// <param name="dbFilePath">The path to the SQLite database file.</param>
        /// <param name="outputDirectory">The directory where the CSV files will be saved. If not specified, the executing assembly's location will be used.</param>
        /// <exception cref="ArgumentNullException">Thrown when the dbFilePath is null or empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the output directory cannot be determined.</exception>
        public static void ExportDatabaseToCsv(string dbFilePath, string outputDirectory = "")
        {
            // Check if the database file path is provided
            if (string.IsNullOrEmpty(dbFilePath)) throw new ArgumentNullException(nameof(dbFilePath));

            // Determine the output directory if not provided
            if (string.IsNullOrEmpty(outputDirectory))
            {
                outputDirectory = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
                                  ?? throw new InvalidOperationException("Could not determine the output directory.");
            }

            // Create the connection string for the SQLite database
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

            // Open a connection to the SQLite database
            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                // Query to get distinct combinations of ProductId and Granularity from the Candles table
                string getCombinationsQuery = "SELECT DISTINCT ProductId, Granularity FROM Candles";
                using (var command = new SqliteCommand(getCombinationsQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    // Loop through each combination of ProductId and Granularity
                    while (reader.Read())
                    {
                        var productId = reader.GetString(0);
                        var granularity = reader.GetString(1);

                        // Export the data for each combination to a CSV file
                        ExportProductGranularityToCsv(connection, productId, granularity, outputDirectory);
                    }
                }
            }
        }

        private static void ExportProductGranularityToCsv(SqliteConnection connection, string productId, string granularity, string outputDirectory)
        {
            // Query to select all records for the specified ProductId and Granularity
            string query = "SELECT * FROM Candles WHERE ProductId = @ProductId AND Granularity = @Granularity";
            using (var command = new SqliteCommand(query, connection))
            {
                // Add parameters to the query
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                // Determine the output CSV file path
                string outputCsvFilePath = Path.Combine(outputDirectory, $"Export_{productId}_{granularity}.csv");

                // Execute the query and write the results to the CSV file
                using (var reader = command.ExecuteReader())
                using (var writer = new StreamWriter(outputCsvFilePath))
                using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true }))
                {
                    bool headerWritten = false;

                    // Loop through each record in the result set
                    while (reader.Read())
                    {
                        // Write the header row if it hasn't been written yet
                        if (!headerWritten)
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (reader.GetName(i) != "Id") // Skip the "Id" field
                                {
                                    csv.WriteField(reader.GetName(i));
                                }
                            }
                            csv.NextRecord();
                            headerWritten = true;
                        }

                        // Write the data row
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.GetName(i) != "Id") // Skip the "Id" field
                            {
                                csv.WriteField(reader[i]);
                            }
                        }
                        csv.NextRecord();
                    }
                }
            }
        }
    }
}
