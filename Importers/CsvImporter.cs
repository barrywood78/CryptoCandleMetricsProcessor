using CsvHelper;
using CsvHelper.Configuration;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;
using CryptoCandleMetricsProcessor.Models;

namespace CryptoCandleMetricsProcessor.Importers
{
    public static class CsvImporter
    {
        /// <summary>
        /// Imports data from a CSV file into a SQLite database table.
        /// </summary>
        /// <param name="csvFilePath">The path to the CSV file.</param>
        /// <param name="dbFilePath">The path to the SQLite database file.</param>
        /// <param name="tableName">The name of the table in the database where data will be inserted.</param>
        /// <param name="mappings">A list of mappings between CSV columns and database fields.</param>
        /// <param name="hasHeaderRecord">Indicates whether the CSV file has a header row.</param>
        public static void ImportCsvToDatabase(string csvFilePath, string dbFilePath, string tableName, List<CsvToDbMapping> mappings, bool hasHeaderRecord)
        {
            // Create the connection string for the SQLite database
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

            // Open a connection to the SQLite database
            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                // Begin a transaction to ensure all data is inserted atomically
                using (var transaction = connection.BeginTransaction())
                {
                    // Open the CSV file for reading
                    using (var reader = new StreamReader(csvFilePath))
                    // Configure the CsvReader
                    using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = hasHeaderRecord }))
                    {
                        bool isFirstRow = true; // Flag to skip the header row if present
                        while (csv.Read())
                        {
                            // Skip the first row if it has headers
                            if (hasHeaderRecord && isFirstRow)
                            {
                                isFirstRow = false;
                                continue;
                            }

                            var values = new List<string>();
                            foreach (var mapping in mappings)
                            {
                                if (mapping.DbFieldName == "StartDate")
                                {
                                    var dateValue = csv.GetField<string>(mapping.CsvColumnIndex);
                                    if (dateValue != null)
                                    {
                                        // Parse and format the date value
                                        DateTime dateTime = DateTime.ParseExact(dateValue, "MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);
                                        values.Add(dateTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                    }
                                    else
                                    {
                                        values.Add(string.Empty); // Add an empty string if the date value is null
                                    }
                                }
                                else
                                {
                                    // Get the field value from the CSV
                                    var fieldValue = csv.GetField(mapping.CsvColumnIndex);
                                    values.Add(fieldValue ?? string.Empty); // Add an empty string if the field value is null
                                }
                            }
                            // Insert the row into the database
                            InsertRow(connection, transaction, tableName, mappings, values);
                        }
                    }

                    // Commit the transaction to save all changes
                    transaction.Commit();
                }
            }
        }

        // Inserts a row into the specified table in the SQLite database
        private static void InsertRow(SqliteConnection connection, SqliteTransaction transaction, string tableName, List<CsvToDbMapping> mappings, List<string> values)
        {
            // Create the columns and parameters strings for the SQL insert statement
            var columns = string.Join(", ", mappings.ConvertAll(m => $"[{m.DbFieldName}]"));
            var parameters = string.Join(", ", mappings.Select((_, index) => $"@p{index}"));

            var commandText = $"INSERT INTO [{tableName}] ({columns}) VALUES ({parameters})";

            using (var command = new SqliteCommand(commandText, connection, transaction))
            {
                // Add the values as parameters to the SQL insert statement
                for (int i = 0; i < values.Count; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", values[i]);
                }

                // Execute the SQL insert statement
                command.ExecuteNonQuery();
            }
        }
    }
}
