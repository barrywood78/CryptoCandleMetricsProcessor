using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class HurstExponent
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int maxLag = 50)
        {
            for (int i = maxLag; i < candles.Count; i++)
            {
                var prices = candles.Skip(i - maxLag).Take(maxLag).Select(c => (double)c.Close).ToList();
                double hurst = CalculateHurst(prices);

                string updateQuery = $@"
                UPDATE {tableName}
                SET HurstExponent = @HurstExponent
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@HurstExponent", hurst);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static double CalculateHurst(List<double> prices)
        {
            var lags = Enumerable.Range(2, 20).ToList();
            var rs = new List<double>();

            foreach (var lag in lags)
            {
                var values = new List<double>();
                for (int i = 0; i < prices.Count - lag; i++)
                {
                    values.Add(Math.Log(prices[i + lag] / prices[i]));
                }

                var mean = values.Average();
                var adjustedValues = values.Select((double v) => v - mean).ToList();
                var cumDeviation = adjustedValues.Select((v, i) => adjustedValues.Take(i + 1).Sum()).ToList();

                var range = cumDeviation.Max() - cumDeviation.Min();
                var stdDev = Math.Sqrt(adjustedValues.Select(v => v * v).Average());

                rs.Add(range / stdDev);
            }

            var logRs = rs.Select((double v) => Math.Log(v)).ToList();
            var logLags = lags.Select(l => Math.Log(l)).ToList();

            double sumXY = 0, sumX = 0, sumY = 0, sumX2 = 0;
            for (int i = 0; i < logRs.Count; i++)
            {
                sumXY += logLags[i] * logRs[i];
                sumX += logLags[i];
                sumY += logRs[i];
                sumX2 += logLags[i] * logLags[i];
            }

            int n = logRs.Count();
            return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        }
    }
}
