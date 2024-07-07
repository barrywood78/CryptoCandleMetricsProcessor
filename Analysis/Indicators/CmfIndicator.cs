using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CmfIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        /// <summary>
        /// Calculates the Chaikin Money Flow (CMF) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the CMF calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var cmfResults = candles.GetCmf(period)
                                    .Where(r => r.Cmf.HasValue)
                                    .Select(r => (DateTicks: r.Date.Ticks, Cmf: r.Cmf!.Value))
                                    .ToList();

            UpdateCmfValues(connection, transaction, tableName, productId, granularity, cmfResults);
        }

        private static void UpdateCmfValues(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, double Cmf)> cmfResults)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET CMF = @CMF
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@CMF", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in cmfResults.Chunk(BatchSize))
            {
                foreach (var (dateTicks, cmf) in batch)
                {
                    command.Parameters["@CMF"].Value = cmf;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}