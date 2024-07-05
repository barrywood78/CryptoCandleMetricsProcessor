using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CompositeMomentumIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int rsiPeriod = 14, int macdFastPeriod = 12, int macdSlowPeriod = 26, int macdSignalPeriod = 9)
        {
            var rsiResults = candles.GetRsi(rsiPeriod).ToList();
            var macdResults = candles.GetMacd(macdFastPeriod, macdSlowPeriod, macdSignalPeriod).ToList();
            var rocResults = candles.GetRoc(14).ToList();

            int startIndex = Math.Max(rsiPeriod, Math.Max(macdSlowPeriod, 14));
            var momentumResults = new List<(long DateTicks, decimal CompositeMomentum)>(candles.Count - startIndex);

            for (int i = startIndex; i < candles.Count; i++)
            {
                decimal rsiComponent = (decimal)((rsiResults[i].Rsi ?? 0) - 50) / 50;
                decimal macdComponent = (decimal)(macdResults[i].Macd ?? 0) / candles[i].Close;
                decimal rocComponent = (decimal)((rocResults[i].Roc ?? 0) / 100);
                decimal compositeMomentum = (rsiComponent + macdComponent + rocComponent) / 3;

                momentumResults.Add((candles[i].Date.Ticks, compositeMomentum));
            }

            UpdateCompositeMomentum(connection, transaction, tableName, productId, granularity, momentumResults);
        }

        private static void UpdateCompositeMomentum(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal CompositeMomentum)> momentumResults)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET CompositeMomentum = @CompositeMomentum
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@CompositeMomentum", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in momentumResults.Chunk(BatchSize))
            {
                foreach (var (dateTicks, compositeMomentum) in batch)
                {
                    command.Parameters["@CompositeMomentum"].Value = (double)compositeMomentum;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}