using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class FibonacciRetracementIndicator
    {
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

            foreach (var candle in candles)
            {
                foreach (var level in fibonacciLevels)
                {
                    var retracementLevel = highestHigh - (highestHigh - lowestLow) * level.Key;

                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET [{level.Value}] = @FibonacciLevel
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@FibonacciLevel", retracementLevel);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candle.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
