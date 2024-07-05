using System;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class BuySignalIndicator
    {
        /// <summary>
        /// Calculates and updates buy scores and signals in the database for the given product and granularity.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            try
            {
                // Step 1: Calculate BuyScores
                CalculateBuyScores(connection, transaction, tableName, productId, granularity);

                // Step 2: Update BuySignals based on BuyScores
                UpdateBuySignals(connection, transaction, tableName, productId, granularity);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in BuySignalIndicator.Calculate: {ex.Message}");
                throw;
            }
        }

        private static void CalculateBuyScores(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            string updateQuery = $@"
            UPDATE {tableName}
            SET BuyScore = (
                -- RSI Score
                (CASE 
                    WHEN RSI > 40 AND RSI < 60 THEN 2 
                    WHEN RSI > 35 AND RSI < 65 THEN 1 
                    ELSE 0 
                END) +
                -- Stochastic Score
                (CASE WHEN Stoch_K < 80 AND Stoch_K > Stoch_D THEN 1 ELSE 0 END) +
                -- ADX Score
                (CASE 
                    WHEN ADX > 25 THEN 3
                    WHEN ADX > 20 THEN 2 
                    ELSE 0 
                END) +
                -- Bollinger Bands Score
                (CASE 
                    WHEN BB_PercentB > 0.3 AND BB_PercentB < 0.7 THEN 2 
                    WHEN BB_PercentB > 0.2 AND BB_PercentB < 0.8 THEN 1 
                    ELSE 0 
                END) +
                -- Chaikin Money Flow Score
                (CASE WHEN CMF > 0 THEN 1 ELSE 0 END) +
                -- MACD Histogram Score
                (CASE 
                    WHEN MACD_Histogram > 0 AND MACD_Histogram > Lagged_MACD_1 THEN 2 
                    WHEN MACD_Histogram > 0 THEN 1 
                    ELSE 0 
                END) +
                -- Accumulation/Distribution Line Score
                (CASE WHEN ADL > Lagged_Close_1 THEN 1 ELSE 0 END) +
                -- EMA vs SMA Score
                (CASE 
                    WHEN EMA > SMA AND EMA > Lagged_EMA_1 THEN 2 
                    WHEN EMA > SMA THEN 1 
                    ELSE 0 
                END) +
                -- MACD vs Signal Line Score
                (CASE WHEN MACD > MACD_Signal THEN 1 ELSE 0 END) +
                -- Relative Volume Score
                (CASE 
                    WHEN RelativeVolume > 2 THEN 3
                    WHEN RelativeVolume > 1 THEN 2
                    WHEN RelativeVolume > 0 THEN 1
                    ELSE 0 
                END) +
                -- ATR Trend Score
                (CASE 
                    WHEN ATR > Lagged_ATR_1 * 1.2 THEN 2
                    WHEN ATR > Lagged_ATR_1 THEN 1
                    ELSE 0
                END) +
                -- Market Regime Score
                (CASE 
                    WHEN MarketRegime = 'Trending Up' THEN 2
                    WHEN MarketRegime = 'Trending Down' THEN -2
                    WHEN MarketRegime = 'Ranging' THEN 0
                    WHEN MarketRegime = 'Transitioning' THEN 1
                    ELSE 0 
                END) +
                -- Candle Pattern Score
                (CASE 
                    WHEN CandlePatternScore >= 8 THEN 3
                    WHEN CandlePatternScore >= 6 THEN 2
                    WHEN CandlePatternScore >= 4 THEN 1
                    ELSE 0
                END)
            )
            WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;

            int rowsAffected = command.ExecuteNonQuery();
            Console.WriteLine($"Rows updated for BuyScore: {rowsAffected}");
        }

        private static void UpdateBuySignals(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            string updateQuery = $@"
            UPDATE {tableName}
            SET BuySignal = CASE WHEN BuyScore >= 16 THEN 1 ELSE 0 END
            WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;

            int rowsAffected = command.ExecuteNonQuery();
            Console.WriteLine($"Rows updated for BuySignal: {rowsAffected}");
        }
    }
}