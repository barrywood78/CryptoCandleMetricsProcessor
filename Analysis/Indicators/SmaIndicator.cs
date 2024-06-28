using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class SmaIndicator
    {
        /// <summary>
        /// Calculates the Simple Moving Average (SMA) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the SMA calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate SMA results using the Skender.Stock.Indicators library
            var smaResults = candles.GetSma(period).ToList();

            // Iterate through each SMA result and update the database
            for (int i = 0; i < smaResults.Count; i++)
            {
                if (smaResults[i].Sma != null) // Only update if SMA value is not null
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET SMA = @SMA
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@SMA", smaResults[i].Sma);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", smaResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
