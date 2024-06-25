using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;

namespace CryptoCandleMetricsProcessor.Exporters
{
    public static class CsvExporter
    {
        public static void ExportDatabaseToCsv(string dbFilePath, string outputDirectory = "")
        {
            if (string.IsNullOrEmpty(dbFilePath)) throw new ArgumentNullException(nameof(dbFilePath));

            if (string.IsNullOrEmpty(outputDirectory))
            {
                outputDirectory = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
                                  ?? throw new InvalidOperationException("Could not determine the output directory.");
            }

            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                string getCombinationsQuery = "SELECT DISTINCT ProductId, Granularity FROM Candles";
                using (var command = new SqliteCommand(getCombinationsQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var productId = reader.GetString(0);
                        var granularity = reader.GetString(1);

                        ExportProductGranularityToCsv(connection, productId, granularity, outputDirectory);
                    }
                }
            }
        }

        private static void ExportProductGranularityToCsv(SqliteConnection connection, string productId, string granularity, string outputDirectory)
        {
            string query = "SELECT * FROM Candles WHERE ProductId = @ProductId AND Granularity = @Granularity";
            using (var command = new SqliteCommand(query, connection))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                string outputCsvFilePath = Path.Combine(outputDirectory, $"Export_{productId}_{granularity}.csv");

                using (var reader = command.ExecuteReader())
                using (var writer = new StreamWriter(outputCsvFilePath))
                using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true }))
                {
                    bool headerWritten = false;

                    while (reader.Read())
                    {
                        if (!headerWritten)
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (reader.GetName(i) != "Id")
                                {
                                    csv.WriteField(reader.GetName(i));
                                }
                            }
                            csv.NextRecord();
                            headerWritten = true;
                        }

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.GetName(i) != "Id")
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
