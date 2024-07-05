using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceActionClassification
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 5)
        {
            var results = new List<(long DateTicks, string Pattern)>();

            for (int i = period; i < candles.Count; i++)
            {
                var recentCandles = candles.Skip(i - period).Take(period);
                string pattern = ClassifyPriceAction(recentCandles);
                results.Add((candles[i].Date.Ticks, pattern));
            }

            UpdatePriceActionPatterns(connection, transaction, tableName, productId, granularity, results);
        }

        private static string ClassifyPriceAction(IEnumerable<Quote> candles)
        {
            bool isUptrend = candles.All(c => c.Close > c.Open);
            bool isDowntrend = candles.All(c => c.Close < c.Open);
            bool isRangebound = Math.Abs(candles.Last().Close - candles.First().Close) / candles.First().Close < 0.01m;

            if (isUptrend) return "Uptrend";
            if (isDowntrend) return "Downtrend";
            if (isRangebound) return "Rangebound";
            return "Mixed";
        }

        private static void UpdatePriceActionPatterns(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, string Pattern)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET PriceActionPattern = @PriceActionPattern
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@PriceActionPattern", SqliteType.Text);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, pattern) in batch)
                {
                    command.Parameters["@PriceActionPattern"].Value = pattern;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}