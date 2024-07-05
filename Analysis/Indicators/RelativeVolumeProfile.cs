using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using MathNet.Numerics.Statistics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RelativeVolumeProfile
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int lookbackPeriod = 30)
        {
            var volumes = candles.Select(c => (double)c.Volume).ToArray();
            var results = new List<(long DateTicks, decimal RelativeVolume, string VolumeProfile)>();

            for (int i = lookbackPeriod; i < candles.Count; i++)
            {
                var recentVolumes = volumes.Skip(i - lookbackPeriod).Take(lookbackPeriod);
                double averageVolume = recentVolumes.Average();
                double stdDevVolume = recentVolumes.StandardDeviation();
                double currentVolume = volumes[i];
                decimal relativeVolume = (decimal)((currentVolume - averageVolume) / stdDevVolume);
                string volumeProfile = CategorizeVolume(relativeVolume);

                results.Add((candles[i].Date.Ticks, relativeVolume, volumeProfile));
            }

            UpdateRelativeVolumeProfiles(connection, transaction, tableName, productId, granularity, results);
        }

        private static string CategorizeVolume(decimal relativeVolume)
        {
            if (relativeVolume > 2) return "Extremely High";
            if (relativeVolume > 1) return "High";
            if (relativeVolume < -2) return "Extremely Low";
            if (relativeVolume < -1) return "Low";
            return "Normal";
        }

        private static void UpdateRelativeVolumeProfiles(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal RelativeVolume, string VolumeProfile)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET RelativeVolume = @RelativeVolume,
                    VolumeProfile = @VolumeProfile
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@RelativeVolume", SqliteType.Real);
            command.Parameters.Add("@VolumeProfile", SqliteType.Text);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, relativeVolume, volumeProfile) in batch)
                {
                    command.Parameters["@RelativeVolume"].Value = (double)relativeVolume;
                    command.Parameters["@VolumeProfile"].Value = volumeProfile;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}