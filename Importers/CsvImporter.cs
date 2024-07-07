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
        public static int ImportCsvToDatabase(string csvFilePath, string dbFilePath, string tableName, List<CsvToDbMapping> mappings, bool hasHeaderRecord)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";
            int rowsImported = 0;

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    using (var reader = new StreamReader(csvFilePath))
                    using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = hasHeaderRecord }))
                    {
                        bool isFirstRow = true;
                        while (csv.Read())
                        {
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
                                        DateTime dateTime = DateTime.ParseExact(dateValue, "MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture);
                                        values.Add(dateTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                    }
                                    else
                                    {
                                        values.Add(string.Empty);
                                    }
                                }
                                else
                                {
                                    var fieldValue = csv.GetField(mapping.CsvColumnIndex);
                                    values.Add(fieldValue ?? string.Empty);
                                }
                            }
                            InsertRow(connection, transaction, tableName, mappings, values);
                            rowsImported++;
                        }
                    }
                    transaction.Commit();
                }
            }
            return rowsImported;
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
