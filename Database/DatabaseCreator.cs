using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Models;

namespace CryptoCandleMetricsProcessor.Database
{
    public static class DatabaseCreator
    {
        public static void CreateDatabaseWithTable(string dbFilePath, string tableName, List<FieldDefinition> fields)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                // Create table if it doesn't exist
                var fieldDefinitions = string.Join(", ", fields.ConvertAll(f => $"[{f.Name}] {f.DataType}"));
                string createTableQuery = $@"
                    CREATE TABLE IF NOT EXISTS [{tableName}] (
                        [Id] INTEGER PRIMARY KEY AUTOINCREMENT,
                        {fieldDefinitions}
                    )";

                using (var command = new SqliteCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Create indexes
                string createIndexQuery = $@"
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_ProductId ON [{tableName}] (ProductId);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_Granularity ON [{tableName}] (Granularity);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartDate ON [{tableName}] (StartDate);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartUnix ON [{tableName}] (StartUnix);
                ";

                using (var command = new SqliteCommand(createIndexQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
