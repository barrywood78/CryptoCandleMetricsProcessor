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
        public static void ImportCsvToDatabase(string csvFilePath, string dbFilePath, string tableName, List<CsvToDbMapping> mappings, bool hasHeaderRecord)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

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
                        }
                    }

                    transaction.Commit();
                }
            }
        }

        private static void InsertRow(SqliteConnection connection, SqliteTransaction transaction, string tableName, List<CsvToDbMapping> mappings, List<string> values)
        {
            var columns = string.Join(", ", mappings.ConvertAll(m => $"[{m.DbFieldName}]"));
            var parameters = string.Join(", ", mappings.Select((_, index) => $"@p{index}"));

            var commandText = $"INSERT INTO [{tableName}] ({columns}) VALUES ({parameters})";

            using (var command = new SqliteCommand(commandText, connection, transaction))
            {
                for (int i = 0; i < values.Count; i++)
                {
                    command.Parameters.AddWithValue($"@p{i}", values[i]);
                }

                command.ExecuteNonQuery();
            }
        }
    }
}
