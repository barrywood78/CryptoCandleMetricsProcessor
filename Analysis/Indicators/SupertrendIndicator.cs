using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class SupertrendIndicator
    {
        /// <summary>
        /// Calculates the SuperTrend indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="atrPeriod">The period to use for the Average True Range (ATR) calculation (default is 14).</param>
        /// <param name="multiplier">The multiplier to use for the SuperTrend calculation (default is 3.0).</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int atrPeriod = 14, decimal multiplier = 3.0m)
        {
            // Calculate SuperTrend results using the Skender.Stock.Indicators library
            var supertrendResults = candles.GetSuperTrend(atrPeriod, (double)multiplier).ToList();

            // Iterate through each SuperTrend result and update the database
            for (int i = 0; i < supertrendResults.Count; i++)
            {
                if (supertrendResults[i].SuperTrend != null) // Only update if SuperTrend value is not null
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET SuperTrend = @SuperTrend
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@SuperTrend", supertrendResults[i].SuperTrend);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", supertrendResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
