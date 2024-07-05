using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CciIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var cciResults = candles.GetCci(period).ToList();
            var validCciResults = new List<(long DateTicks, double Cci)>(cciResults.Count - period + 1);

            for (int i = period - 1; i < cciResults.Count; i++)
            {
                if (cciResults[i].Cci.HasValue)
                {
                    validCciResults.Add((cciResults[i].Date.Ticks, cciResults[i].Cci.Value));
                }
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET CCI = @CCI
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@CCI", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in validCciResults.Chunk(BatchSize))
            {
                foreach (var (dateTicks, cci) in batch)
                {
                    command.Parameters["@CCI"].Value = cci;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}