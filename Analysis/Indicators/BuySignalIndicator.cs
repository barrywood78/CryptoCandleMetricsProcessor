using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class BuySignalIndicator
    {
        /// <summary>
        /// Calculates and updates buy signals in the database for the given product and granularity.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            // Step 1: Calculate BuyScores
            CalculateBuyScores(connection, transaction, tableName, productId, granularity);

            // Step 2: Retrieve all BuyScores
            var allScores = RetrieveAllBuyScores(connection, transaction, tableName, productId, granularity);

            // Step 3: Calculate and update BuySignals
            UpdateBuySignals(connection, transaction, tableName, productId, granularity, allScores);
        }

        // Calculate BuyScores based on predefined rules and update the database
        private static void CalculateBuyScores(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            string updateQuery = $@"
            UPDATE {tableName}
            SET BuyScore = 
                (CASE 
                    WHEN RSI > 40 AND RSI < 60 THEN 2 
                    WHEN RSI > 35 AND RSI < 65 THEN 1 
                    ELSE 0 
                END) +
                (CASE WHEN Stoch_K < 80 AND Stoch_K > Stoch_D THEN 1 ELSE 0 END) +
                (CASE 
                    WHEN ADX > 25 THEN 3
                    WHEN ADX > 20 THEN 2 
                    ELSE 0 
                END) +
                (CASE 
                    WHEN BB_PercentB > 0.3 AND BB_PercentB < 0.7 THEN 2 
                    WHEN BB_PercentB > 0.2 AND BB_PercentB < 0.8 THEN 1 
                    ELSE 0 
                END) +
                (CASE WHEN CMF > 0 THEN 1 ELSE 0 END) +
                (CASE 
                    WHEN MACD_Histogram > 0 AND MACD_Histogram > Lagged_MACD_1 THEN 2 
                    WHEN MACD_Histogram > 0 THEN 1 
                    ELSE 0 
                END) +
                (CASE WHEN ADL > Lagged_Close_1 THEN 1 ELSE 0 END) +
                (CASE 
                    WHEN EMA > SMA AND EMA > Lagged_EMA_1 THEN 2 
                    WHEN EMA > SMA THEN 1 
                    ELSE 0 
                END) +
                (CASE WHEN MACD > MACD_Signal THEN 1 ELSE 0 END) +
                (CASE WHEN Volume > Lagged_Close_1 THEN 1 ELSE 0 END) +
                (CASE 
                    WHEN ATR > Lagged_ATR_1 * 1.2 THEN 2
                    WHEN ATR > Lagged_ATR_1 THEN 1
                    ELSE 0
                END)
            WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using (var command = new SqliteCommand(updateQuery, connection, transaction))
            {
                // Add parameters to the update command
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);
                // Execute the update command
                command.ExecuteNonQuery();
            }
        }

        // Retrieve all BuyScores from the database for the given product and granularity
        private static List<int> RetrieveAllBuyScores(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            var scores = new List<int>();
            string selectQuery = $"SELECT BuyScore FROM {tableName} WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using (var command = new SqliteCommand(selectQuery, connection, transaction))
            {
                // Add parameters to the select command
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                using (var reader = command.ExecuteReader())
                {
                    // Read and add each BuyScore to the list
                    while (reader.Read())
                    {
                        scores.Add(reader.GetInt32(0));
                    }
                }
            }

            return scores;
        }

        // Update BuySignals in the database based on calculated BuyScores
        private static void UpdateBuySignals(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<int> allScores)
        {
            string updateQuery = $@"
                                UPDATE {tableName}
                                SET BuySignal = @BuySignal
                                WHERE ProductId = @ProductId
                                  AND Granularity = @Granularity
                                  AND BuyScore = @BuyScore";

            using (var command = new SqliteCommand(updateQuery, connection, transaction))
            {
                foreach (var score in allScores.Distinct())
                {
                    var buySignal = score >= 14 ? 1 : 0;

                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@BuySignal", buySignal);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@BuyScore", score);
                    command.ExecuteNonQuery();
                }
            }
        }

    }
}
