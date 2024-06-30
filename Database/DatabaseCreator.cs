using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Models;

namespace CryptoCandleMetricsProcessor.Database
{
    public static class DatabaseCreator
    {
        /// <summary>
        /// Creates a SQLite database with the specified table and fields if it does not already exist.
        /// </summary>
        /// <param name="dbFilePath">The path to the SQLite database file.</param>
        /// <param name="tableName">The name of the table to create in the database.</param>
        /// <param name="fields">A list of field definitions for the table.</param>
        public static void CreateDatabaseWithTable(string dbFilePath, string tableName, List<FieldDefinition> fields)
        {
            // Create the connection string for the SQLite database
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";

            // Open a connection to the SQLite database
            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();

                // Generate the SQL for creating the table with the specified fields
                var fieldDefinitions = string.Join(", ", fields.ConvertAll(f => $"[{f.Name}] {f.DataType}"));
                string createTableQuery = $@"
                    CREATE TABLE IF NOT EXISTS [{tableName}] (
                        [Id] INTEGER PRIMARY KEY AUTOINCREMENT,
                        {fieldDefinitions}
                    )";

                // Execute the create table query
                using (var command = new SqliteCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Generate the SQL for creating indexes on the table
                string createIndexQuery = $@"
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_ProductId ON [{tableName}] (ProductId);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_Granularity ON [{tableName}] (Granularity);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartDate ON [{tableName}] (StartDate);
                    CREATE INDEX IF NOT EXISTS idx_{tableName}_StartUnix ON [{tableName}] (StartUnix);
                    ANALYZE;
                ";

                // Execute the create indexes query
                using (var command = new SqliteCommand(createIndexQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
