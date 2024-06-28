using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceUpIndicator
    {
        /// <summary>
        /// Calculates whether the price is up (1) or not (0) compared to the previous period and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Iterate through each candle starting from the second one
            for (int i = 1; i < candles.Count; i++)
            {
                // Determine if the price is up compared to the previous candle
                int priceUp = candles[i].Close > candles[i - 1].Close ? 1 : 0;

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET PriceUp = @PriceUp
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    // Add parameters to the update command
                    command.Parameters.AddWithValue("@PriceUp", priceUp);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                    // Execute the update command
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
