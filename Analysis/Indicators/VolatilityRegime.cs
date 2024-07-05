using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using MathNet.Numerics.Statistics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class VolatilityRegime
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            var atrResults = candles.GetAtr(period).ToList();
            var results = new List<(long DateTicks, string Regime)>();

            for (int i = period; i < candles.Count; i++)
            {
                var recentATRs = atrResults.Skip(i - period).Take(period).Select(r => r.Atr ?? 0).ToArray();
                double meanATR = recentATRs.Mean();
                double stdDevATR = recentATRs.StandardDeviation();

                string regime = DetermineRegime(atrResults[i].Atr ?? 0, meanATR, stdDevATR);
                results.Add((candles[i].Date.Ticks, regime));
            }

            UpdateVolatilityRegimes(connection, transaction, tableName, productId, granularity, results);
        }

        private static string DetermineRegime(double atr, double meanATR, double stdDevATR)
        {
            if (atr > meanATR + stdDevATR) return "High";
            if (atr < meanATR - stdDevATR) return "Low";
            return "Medium";
        }

        private static void UpdateVolatilityRegimes(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, string Regime)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET VolatilityRegime = @VolatilityRegime
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@VolatilityRegime", SqliteType.Text);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, regime) in batch)
                {
                    command.Parameters["@VolatilityRegime"].Value = regime;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}