using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class StochasticIndicator
    {
        /// <summary>
        /// Calculates the Stochastic Oscillator indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the Stochastic Oscillator calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate Stochastic Oscillator results using the Skender.Stock.Indicators library
            var stochasticResults = candles.GetStoch(period).ToList();

            // Iterate through each Stochastic Oscillator result and update the database
            for (int i = 0; i < stochasticResults.Count; i++)
            {
                if (stochasticResults[i].K != null && stochasticResults[i].D != null) // Only update if %K and %D values are not null
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET Stoch_K = @Stoch_K, Stoch_D = @Stoch_D
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@Stoch_K", stochasticResults[i].K);
                        command.Parameters.AddWithValue("@Stoch_D", stochasticResults[i].D);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", stochasticResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
