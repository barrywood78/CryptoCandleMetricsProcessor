using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class AtrIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        /// <summary>
        /// Calculates the Average True Range (ATR) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the ATR calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate ATR results using the Skender.Stock.Indicators library
            var atrResults = candles.GetAtr(period)
                                    .Where(r => r.Atr.HasValue)
                                    .Select(r => new { r.Date, Atr = r.Atr.Value })
                                    .ToList();

            string updateQuery = $@"
                UPDATE {tableName}
                SET ATR = @ATR
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ATR", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in atrResults.Chunk(BatchSize))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@ATR"].Value = result.Atr;
                    command.Parameters["@StartDate"].Value = result.Date.Ticks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}