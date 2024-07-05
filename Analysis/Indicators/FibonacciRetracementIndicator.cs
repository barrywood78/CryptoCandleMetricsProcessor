using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class FibonacciRetracementIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates Fibonacci retracement levels and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Define the Fibonacci levels
            var fibonacciLevels = new Dictionary<decimal, string>
            {
                { 0.236m, "FibRetracement_23_6" },
                { 0.382m, "FibRetracement_38_2" },
                { 0.5m, "FibRetracement_50" },
                { 0.618m, "FibRetracement_61_8" },
                { 0.764m, "FibRetracement_78_6" }
            };

            // Determine the highest high and lowest low in the period
            var highestHigh = candles.Max(c => c.High);
            var lowestLow = candles.Min(c => c.Low);

            // Prepare results for batch update
            var results = new List<(long DateTicks, Dictionary<string, decimal> Levels)>();

            foreach (var candle in candles)
            {
                var levels = new Dictionary<string, decimal>();

                foreach (var level in fibonacciLevels)
                {
                    // Calculate the retracement level
                    var retracementLevel = highestHigh - (highestHigh - lowestLow) * level.Key;
                    levels[level.Value] = retracementLevel;
                }

                results.Add((candle.Date.Ticks, levels));
            }

            // Update database in batches
            foreach (var batch in results
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                UpdateDatabase(connection, transaction, tableName, productId, granularity, batch);
            }
        }

        private static void UpdateDatabase(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, Dictionary<string, decimal> Levels)> batch)
        {
            foreach (var level in batch.First().Levels.Keys)
            {
                string updateQuery = $@"
                    UPDATE {tableName}
                    SET [{level}] = @FibonacciLevel
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

                using var command = new SqliteCommand(updateQuery, connection, transaction);
                command.Parameters.Add("@FibonacciLevel", SqliteType.Real);
                command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
                command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
                command.Parameters.Add("@StartDate", SqliteType.Integer);

                foreach (var result in batch)
                {
                    command.Parameters["@FibonacciLevel"].Value = result.Levels[level];
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
