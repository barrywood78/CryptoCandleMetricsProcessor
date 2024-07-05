using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class BollingerBandsIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

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
            var bbResults = candles.GetBollingerBands(period)
                                   .Where(r => r.Sma.HasValue && r.UpperBand.HasValue && r.LowerBand.HasValue)
                                   .Select(r => new
                                   {
                                       r.Date,
                                       r.Sma,
                                       r.UpperBand,
                                       r.LowerBand,
                                       r.PercentB,
                                       r.ZScore,
                                       r.Width
                                   })
                                   .ToList();

            string updateQuery = $@"
                UPDATE {tableName}
                SET BB_SMA = @BB_SMA, 
                    BB_UpperBand = @BB_UpperBand, 
                    BB_LowerBand = @BB_LowerBand, 
                    BB_PercentB = @BB_PercentB, 
                    BB_ZScore = @BB_ZScore, 
                    BB_Width = @BB_Width
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@BB_SMA", SqliteType.Real);
            command.Parameters.Add("@BB_UpperBand", SqliteType.Real);
            command.Parameters.Add("@BB_LowerBand", SqliteType.Real);
            command.Parameters.Add("@BB_PercentB", SqliteType.Real);
            command.Parameters.Add("@BB_ZScore", SqliteType.Real);
            command.Parameters.Add("@BB_Width", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in bbResults.Chunk(BatchSize))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@BB_SMA"].Value = result.Sma;
                    command.Parameters["@BB_UpperBand"].Value = result.UpperBand;
                    command.Parameters["@BB_LowerBand"].Value = result.LowerBand;
                    command.Parameters["@BB_PercentB"].Value = result.PercentB;
                    command.Parameters["@BB_ZScore"].Value = result.ZScore;
                    command.Parameters["@BB_Width"].Value = result.Width;
                    command.Parameters["@StartDate"].Value = result.Date.Ticks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}