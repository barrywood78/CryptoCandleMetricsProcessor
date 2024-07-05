using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MarketEfficiencyRatio
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var results = new List<(long DateTicks, decimal MER)>();
            var closePrices = candles.Select(c => c.Close).ToArray();

            for (int i = period; i < candles.Count; i++)
            {
                decimal netPriceChange = Math.Abs(closePrices[i] - closePrices[i - period]);
                decimal sumPriceChanges = 0;

                for (int j = i - period + 1; j <= i; j++)
                {
                    sumPriceChanges += Math.Abs(closePrices[j] - closePrices[j - 1]);
                }

                decimal mer = sumPriceChanges == 0 ? 1 : netPriceChange / sumPriceChanges;
                results.Add((candles[i].Date.Ticks, mer));
            }

            UpdateMarketEfficiencyRatios(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateMarketEfficiencyRatios(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal MER)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET MarketEfficiencyRatio = @MarketEfficiencyRatio
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@MarketEfficiencyRatio", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, mer) in batch)
                {
                    command.Parameters["@MarketEfficiencyRatio"].Value = (double)mer;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}