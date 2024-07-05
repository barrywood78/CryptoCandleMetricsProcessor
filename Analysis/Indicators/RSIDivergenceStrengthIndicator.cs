using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RSIDivergenceStrengthIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var rsiResults = candles.GetRsi(period).ToList();
            var results = new List<(long DateTicks, decimal DivergenceStrength)>();

            for (int i = period; i < candles.Count; i++)
            {
                decimal priceChange = (candles[i].Close - candles[i - period].Close) / candles[i - period].Close;
                double rsiChange = (rsiResults[i].Rsi ?? 0) - (rsiResults[i - period].Rsi ?? 0);
                decimal divergenceStrength = (decimal)(Math.Sign(priceChange) != Math.Sign(rsiChange) ? Math.Abs(priceChange - (decimal)rsiChange) : 0);

                results.Add((candles[i].Date.Ticks, divergenceStrength));
            }

            UpdateRSIDivergenceStrengths(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateRSIDivergenceStrengths(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal DivergenceStrength)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET RSIDivergenceStrength = @RSIDivergenceStrength
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@RSIDivergenceStrength", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, divergenceStrength) in batch)
                {
                    command.Parameters["@RSIDivergenceStrength"].Value = (double)divergenceStrength;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}