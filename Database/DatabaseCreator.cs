using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Models;

namespace CryptoCandleMetricsProcessor.Database
{
    public static class DatabaseCreator
    {
        public static async Task CreateDatabaseWithTableAsync(string dbFilePath, string tableName, List<FieldDefinition> fields)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";
            await using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync();

            try
            {
                var fieldDefinitions = string.Join(", ", fields.ConvertAll(f => $"[{f.Name}] {f.DataType}"));
                string createTableQuery = $@"
                    CREATE TABLE IF NOT EXISTS [{tableName}] (
                        [Id] INTEGER PRIMARY KEY AUTOINCREMENT,
                        {fieldDefinitions}
                    )";

                await using (var command = new SqliteCommand(createTableQuery, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }

                string createIndexQuery = $@"
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_ProductId ON [{tableName}] (ProductId);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_Granularity ON [{tableName}] (Granularity);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartDate ON [{tableName}] (StartDate);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartUnix ON [{tableName}] (StartUnix);
                    ANALYZE;
                ";

                await using (var command = new SqliteCommand(createIndexQuery, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                // Log the exception or handle it as needed
                Console.WriteLine($"Error creating database: {ex.Message}");
                throw;
            }
        }
    }
}