using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class DynamicSupportResistanceIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int lookbackPeriod = 20)
        {
            var results = new List<(long DateTicks, decimal SupportLevel, decimal ResistanceLevel, decimal DistanceToSupport, decimal DistanceToResistance)>();

            for (int i = lookbackPeriod; i < candles.Count; i++)
            {
                var recentCandles = candles.Skip(i - lookbackPeriod).Take(lookbackPeriod);
                decimal supportLevel = recentCandles.Min(c => c.Low);
                decimal resistanceLevel = recentCandles.Max(c => c.High);
                decimal currentClose = candles[i].Close;

                decimal distanceToSupport = (currentClose - supportLevel) / currentClose;
                decimal distanceToResistance = (resistanceLevel - currentClose) / currentClose;

                results.Add((candles[i].Date.Ticks, supportLevel, resistanceLevel, distanceToSupport, distanceToResistance));
            }

            UpdateDynamicSupportResistance(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateDynamicSupportResistance(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal SupportLevel, decimal ResistanceLevel, decimal DistanceToSupport, decimal DistanceToResistance)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET DynamicSupportLevel = @SupportLevel,
                    DynamicResistanceLevel = @ResistanceLevel,
                    DistanceToDynamicSupport = @DistanceToSupport,
                    DistanceToDynamicResistance = @DistanceToResistance
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@SupportLevel", SqliteType.Real);
            command.Parameters.Add("@ResistanceLevel", SqliteType.Real);
            command.Parameters.Add("@DistanceToSupport", SqliteType.Real);
            command.Parameters.Add("@DistanceToResistance", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, supportLevel, resistanceLevel, distanceToSupport, distanceToResistance) in batch)
                {
                    command.Parameters["@SupportLevel"].Value = (double)supportLevel;
                    command.Parameters["@ResistanceLevel"].Value = (double)resistanceLevel;
                    command.Parameters["@DistanceToSupport"].Value = (double)distanceToSupport;
                    command.Parameters["@DistanceToResistance"].Value = (double)distanceToResistance;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}