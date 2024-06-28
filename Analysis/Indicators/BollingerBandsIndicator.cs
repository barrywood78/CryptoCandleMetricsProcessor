using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class BollingerBandsIndicator
    {
        /// <summary>
        /// Calculates the Bollinger Bands indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the Bollinger Bands calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate Bollinger Bands results using the Skender.Stock.Indicators library
            var bbResults = candles.GetBollingerBands(period).ToList();

            // Iterate through each Bollinger Bands result and update the database
            for (int i = 0; i < bbResults.Count; i++)
            {
                if (bbResults[i].Sma != null && bbResults[i].UpperBand != null && bbResults[i].LowerBand != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET BB_SMA = @BB_SMA, BB_UpperBand = @BB_UpperBand, BB_LowerBand = @BB_LowerBand, BB_PercentB = @BB_PercentB, BB_ZScore = @BB_ZScore, BB_Width = @BB_Width
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@BB_SMA", bbResults[i].Sma);
                        command.Parameters.AddWithValue("@BB_UpperBand", bbResults[i].UpperBand);
                        command.Parameters.AddWithValue("@BB_LowerBand", bbResults[i].LowerBand);
                        command.Parameters.AddWithValue("@BB_PercentB", bbResults[i].PercentB);
                        command.Parameters.AddWithValue("@BB_ZScore", bbResults[i].ZScore);
                        command.Parameters.AddWithValue("@BB_Width", bbResults[i].Width);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", bbResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
