using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class DynamicSupportResistanceIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int lookbackPeriod = 20)
        {
            for (int i = lookbackPeriod; i < candles.Count; i++)
            {
                var recentCandles = candles.Skip(i - lookbackPeriod).Take(lookbackPeriod).ToList();

                decimal supportLevel = CalculateSupportLevel(recentCandles);
                decimal resistanceLevel = CalculateResistanceLevel(recentCandles);

                decimal distanceToSupport = (candles[i].Close - supportLevel) / candles[i].Close;
                decimal distanceToResistance = (resistanceLevel - candles[i].Close) / candles[i].Close;

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET DynamicSupportLevel = @SupportLevel,
                        DynamicResistanceLevel = @ResistanceLevel,
                        DistanceToDynamicSupport = @DistanceToSupport,
                        DistanceToDynamicResistance = @DistanceToResistance
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@SupportLevel", supportLevel);
                    command.Parameters.AddWithValue("@ResistanceLevel", resistanceLevel);
                    command.Parameters.AddWithValue("@DistanceToSupport", distanceToSupport);
                    command.Parameters.AddWithValue("@DistanceToResistance", distanceToResistance);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static decimal CalculateSupportLevel(List<Quote> candles)
        {
            return candles.Min(c => c.Low);
        }

        private static decimal CalculateResistanceLevel(List<Quote> candles)
        {
            return candles.Max(c => c.High);
        }
    }
}