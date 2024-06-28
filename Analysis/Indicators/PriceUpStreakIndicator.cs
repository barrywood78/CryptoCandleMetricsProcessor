using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceUpStreakIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            int priceUpStreak = 0; // Initialize the price up streak counter

            for (int i = 1; i < candles.Count; i++)
            {
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

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET PriceUpStreak = @PriceUpStreak
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@PriceUpStreak", priceUpStreak);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
