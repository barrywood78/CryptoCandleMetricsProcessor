using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class IchimokuIndicator
    {
        /// <summary>
        /// Calculates the Ichimoku Cloud indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate Ichimoku results using the Skender.Stock.Indicators library
            var ichimokuResults = candles.GetIchimoku().ToList();

            // Iterate through each Ichimoku result and update the database
            for (int i = 0; i < ichimokuResults.Count; i++)
            {
                if (ichimokuResults[i] != null) // Only update if Ichimoku result is not null
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET Ichimoku_TenkanSen = @TenkanSen,
                            Ichimoku_KijunSen = @KijunSen,
                            Ichimoku_SenkouSpanA = @SenkouSpanA,
                            Ichimoku_SenkouSpanB = @SenkouSpanB,
                            Ichimoku_ChikouSpan = @ChikouSpan
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters to the update command
                        command.Parameters.AddWithValue("@TenkanSen", ichimokuResults[i].TenkanSen ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@KijunSen", ichimokuResults[i].KijunSen ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SenkouSpanA", ichimokuResults[i].SenkouSpanA ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SenkouSpanB", ichimokuResults[i].SenkouSpanB ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@ChikouSpan", ichimokuResults[i].ChikouSpan ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", ichimokuResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
