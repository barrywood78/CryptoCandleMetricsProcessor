using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class AdLineIndicator
    {
        /// <summary>
        /// Calculates the Accumulation/Distribution Line (ADL) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate ADL results using the Skender.Stock.Indicators library
            var adLineResults = candles.GetAdl().ToList();

            // Iterate through each ADL result and update the database
            for (int i = 0; i < adLineResults.Count; i++)
            {
                string updateQuery = $@"
                    UPDATE {tableName}
                    SET ADL = @ADL
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    // Add parameters to the update command
                    command.Parameters.AddWithValue("@ADL", adLineResults[i].Adl);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", adLineResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                    // Execute the update command
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
