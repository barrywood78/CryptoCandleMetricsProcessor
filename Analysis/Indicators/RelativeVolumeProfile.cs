using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RelativeVolumeProfile
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int lookbackPeriod = 30)
        {
            for (int i = lookbackPeriod; i < candles.Count; i++)
            {
                var recentVolumes = candles.Skip(i - lookbackPeriod).Take(lookbackPeriod).Select(c => c.Volume).ToList();
                decimal averageVolume = recentVolumes.Average();
                decimal stdDevVolume = (decimal)Math.Sqrt(recentVolumes.Select(v => Math.Pow((double)(v - averageVolume), 2)).Average());

                decimal currentVolume = candles[i].Volume;
                decimal relativeVolume = (currentVolume - averageVolume) / stdDevVolume;

                string volumeProfile;
                if (relativeVolume > 2) volumeProfile = "Extremely High";
                else if (relativeVolume > 1) volumeProfile = "High";
                else if (relativeVolume < -1) volumeProfile = "Low";
                else if (relativeVolume < -2) volumeProfile = "Extremely Low";
                else volumeProfile = "Normal";

                string updateQuery = $@"
                UPDATE {tableName}
                SET RelativeVolume = @RelativeVolume,
                    VolumeProfile = @VolumeProfile
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@RelativeVolume", relativeVolume);
                    command.Parameters.AddWithValue("@VolumeProfile", volumeProfile);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
