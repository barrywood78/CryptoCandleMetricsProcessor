using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceUpStreakIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the price up streak (consecutive periods where the closing price is higher than the previous period)
        /// and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            int priceUpStreak = 0; // Initialize the price up streak counter
            var priceUpStreakResults = new List<(long DateTicks, int PriceUpStreak)>();

            // Iterate through each candle starting from the second one
            for (int i = 1; i < candles.Count; i++)
            {
                // Determine if the price is up compared to the previous candle
                int priceUp = candles[i].Close > candles[i - 1].Close ? 1 : 0;

                // Update the streak counter
                if (priceUp == 1)
                {
                    priceUpStreak++;
                }
                else
                {
                    priceUpStreak = 0;
                }

                priceUpStreakResults.Add((candles[i].Date.Ticks, priceUpStreak));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET PriceUpStreak = @PriceUpStreak
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@PriceUpStreak", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in priceUpStreakResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@PriceUpStreak"].Value = result.PriceUpStreak;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
