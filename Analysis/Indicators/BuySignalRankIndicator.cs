﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class BuySignalIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            // Step 1: Calculate BuyScores
            CalculateBuyScores(connection, transaction, tableName, productId, granularity);

            // Step 2: Retrieve all BuyScores
            var allScores = RetrieveAllBuyScores(connection, transaction, tableName, productId, granularity);

            // Step 3: Calculate and update BuySignals
            UpdateBuySignals(connection, transaction, tableName, productId, granularity, allScores);
        }

        private static void CalculateBuyScores(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET BuyScore = 
                    (CASE WHEN RSI > 45 AND RSI < 55 THEN 2 WHEN RSI > 40 AND RSI < 60 THEN 1 ELSE 0 END) +
                    (CASE WHEN Stoch_K < 80 AND Stoch_K > Stoch_D THEN 1 ELSE 0 END) +
                    (CASE WHEN ADX > 20 THEN 2 ELSE 0 END) +
                    (CASE WHEN BB_PercentB > 0.35 AND BB_PercentB < 0.65 THEN 2 WHEN BB_PercentB > 0.25 AND BB_PercentB < 0.75 THEN 1 ELSE 0 END) +
                    (CASE WHEN CMF > 0 THEN 1 ELSE 0 END) +
                    (CASE WHEN MACD_Histogram > 0 AND MACD_Histogram > Lagged_MACD_1 THEN 2 WHEN MACD_Histogram > 0 THEN 1 ELSE 0 END) +
                    (CASE WHEN ADL > Lagged_Close_1 THEN 1 ELSE 0 END) +
                    (CASE WHEN EMA > SMA THEN 1 ELSE 0 END) +
                    (CASE WHEN MACD > MACD_Signal THEN 1 ELSE 0 END)
                WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using (var command = new SqliteCommand(updateQuery, connection, transaction))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);
                command.ExecuteNonQuery();
            }
        }

        private static List<int> RetrieveAllBuyScores(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            var scores = new List<int>();
            string selectQuery = $"SELECT BuyScore FROM {tableName} WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using (var command = new SqliteCommand(selectQuery, connection, transaction))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scores.Add(reader.GetInt32(0));
                    }
                }
            }

            return scores;
        }

        private static void UpdateBuySignals(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<int> allScores)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET FixedBuySignalRank = @FixedRank,
                    PercentileBuySignalRank = @PercentileRank
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND BuyScore = @BuyScore";

            using (var command = new SqliteCommand(updateQuery, connection, transaction))
            {
                foreach (var score in allScores.Distinct())
                {
                    var (_, fixedRank, percentileRank) = CalculateBuySignals(score, allScores);

                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@FixedRank", fixedRank);
                    command.Parameters.AddWithValue("@PercentileRank", percentileRank);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@BuyScore", score);
                    command.ExecuteNonQuery();
                }
            }
        }

        public static (int BuyScore, int FixedRank, int PercentileRank) CalculateBuySignals(int buyScore, List<int> allScores)
        {
            int fixedRank = DetermineFixedBuySignalRank(buyScore);
            int percentileRank = DeterminePercentileBuySignalRank(buyScore, allScores);
            return (buyScore, fixedRank, percentileRank);
        }

        private static int DetermineFixedBuySignalRank(int buyScore)
        {
            if (buyScore >= 10) return 3; // Strong Buy
            if (buyScore >= 7) return 2;  // Moderate Buy
            if (buyScore >= 4) return 1;  // Weak Buy
            return 0;                     // No Signal
        }

        private static int DeterminePercentileBuySignalRank(int buyScore, List<int> allScores)
        {
            int totalCandles = allScores.Count;
            var sortedScores = allScores.OrderByDescending(s => s).ToList();

            if (buyScore >= sortedScores[(int)(totalCandles * 0.03)]) return 3; // Strong Buy (Top 3%)
            if (buyScore >= sortedScores[(int)(totalCandles * 0.15)]) return 2; // Moderate Buy (Top 15%)
            if (buyScore >= sortedScores[(int)(totalCandles * 0.45)]) return 1; // Weak Buy (Top 45%)
            return 0; // No Signal
        }
    }
}