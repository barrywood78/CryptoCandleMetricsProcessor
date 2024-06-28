using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class FibonacciRetracementIndicator
    {
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

            // Iterate through each candle
            foreach (var candle in candles)
            {
                // Iterate through each Fibonacci level
                foreach (var level in fibonacciLevels)
                {
                    // Calculate the retracement level
                    var retracementLevel = highestHigh - (highestHigh - lowestLow) * level.Key;

                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET [{level.Value}] = @FibonacciLevel
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@FibonacciLevel", retracementLevel);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candle.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
