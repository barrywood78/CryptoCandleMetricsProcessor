using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ADLChangeIndicator
    {
        private const int BatchSize = 50000; // Increased batch size for fewer database operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var adlResults = candles.GetAdl().ToList();
            var adlChanges = new List<(long DateTicks, double Change)>(adlResults.Count - 1);

            for (int i = 1; i < adlResults.Count; i++)
            {
                double adlChange = adlResults[i].Adl - adlResults[i - 1].Adl;
                adlChanges.Add((candles[i].Date.Ticks, adlChange));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET ADLChange = @ADLChange
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ADLChange", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in adlChanges.Chunk(BatchSize))
            {
                foreach (var (dateTicks, change) in batch)
                {
                    command.Parameters["@ADLChange"].Value = change;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}