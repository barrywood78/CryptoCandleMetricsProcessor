using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ATRPercentIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var atrResults = candles.GetAtr(period).ToList();
            var atrPercentResults = new List<(long DateTicks, double ATRPercent)>();

            for (int i = period - 1; i < candles.Count; i++)
            {
                if (atrResults[i].Atr.HasValue)
                {
                    double atrPercent = (double)atrResults[i].Atr.Value / (double)candles[i].Close;
                    atrPercentResults.Add((candles[i].Date.Ticks, atrPercent));
                }
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET ATRPercent = @ATRPercent
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ATRPercent", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in atrPercentResults.Chunk(BatchSize))
            {
                foreach (var (dateTicks, atrPercent) in batch)
                {
                    command.Parameters["@ATRPercent"].Value = atrPercent;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}