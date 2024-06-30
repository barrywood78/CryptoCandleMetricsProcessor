using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class FractalDimensionIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int window = 30)
        {
            for (int i = window; i < candles.Count; i++)
            {
                var prices = candles.Skip(i - window).Take(window).Select(c => (double)c.Close).ToList();
                double fractalDimension = CalculateHiguchiFractalDimension(prices);

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET FractalDimension = @FractalDimension
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@FractalDimension", fractalDimension);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static double CalculateHiguchiFractalDimension(List<double> data, int kmax = 10)
        {
            var lnLengths = new List<double>();
            var lnKs = new List<double>();

            for (int k = 1; k <= kmax; k++)
            {
                double length = 0;
                int m = 0;
                while (m < k)
                {
                    for (int i = 0; i < (data.Count - m - 1) / k; i++)
                    {
                        length += Math.Abs(data[m + (i + 1) * k] - data[m + i * k]);
                    }
                    m++;
                }
                length *= (data.Count - 1) / (Math.Pow(k, 2) * Math.Floor((double)(data.Count - m) / k));
                lnLengths.Add(Math.Log(length));
                lnKs.Add(Math.Log(1.0 / k));
            }

            // Linear regression to find the slope
            double sumX = lnKs.Sum();
            double sumY = lnLengths.Sum();
            double sumXY = lnKs.Zip(lnLengths, (x, y) => x * y).Sum();
            double sumX2 = lnKs.Select(x => x * x).Sum();
            int n = lnKs.Count;

            double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            return slope;
        }
    }
}