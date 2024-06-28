using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MacdIndicator
    {
        /// <summary>
        /// Calculates the Moving Average Convergence Divergence (MACD) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate MACD results using the Skender.Stock.Indicators library
            var macdResults = candles.GetMacd().ToList();

            // Iterate through each MACD result and update the database
            for (int i = 0; i < macdResults.Count; i++)
            {
                if (macdResults[i].Macd != null && macdResults[i].Signal != null && macdResults[i].Histogram != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET MACD = @MACD, MACD_Signal = @MACD_Signal, MACD_Histogram = @MACD_Histogram
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@MACD", macdResults[i].Macd);
                        command.Parameters.AddWithValue("@MACD_Signal", macdResults[i].Signal);
                        command.Parameters.AddWithValue("@MACD_Histogram", macdResults[i].Histogram);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", macdResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
