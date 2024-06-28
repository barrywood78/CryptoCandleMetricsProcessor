using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ParabolicSarIndicator
    {
        /// <summary>
        /// Calculates the Parabolic SAR indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate Parabolic SAR results using the Skender.Stock.Indicators library
            var parabolicSarResults = candles.GetParabolicSar().ToList();

            // Iterate through each Parabolic SAR result and update the database
            for (int i = 0; i < parabolicSarResults.Count; i++)
            {
                if (parabolicSarResults[i].Sar != null) // Only update if SAR value is not null
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET ParabolicSar = @ParabolicSar
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@ParabolicSar", parabolicSarResults[i].Sar);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", parabolicSarResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
