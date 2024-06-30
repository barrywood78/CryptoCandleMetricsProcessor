using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using Microsoft.Data.Sqlite;
using MathNet.Numerics.IntegralTransforms;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CyclicalPatternsIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int cyclePeriod = 365)
        {
            // Ensure we have enough data for a full cycle
            if (candles.Count < cyclePeriod) return;

            for (int i = cyclePeriod; i < candles.Count; i++)
            {
                var cycleData = candles.Skip(i - cyclePeriod).Take(cyclePeriod).Select(c => (double)c.Close).ToArray();

                // Detrend the data
                var trend = LinearTrend(cycleData);
                var detrended = cycleData.Select((x, idx) => x - trend.Item1 - trend.Item2 * idx).ToArray();

                // Perform FFT
                var complex = detrended.Select(x => new System.Numerics.Complex(x, 0)).ToArray();
                Fourier.Forward(complex, FourierOptions.Matlab);

                // Find dominant cycle
                int dominantPeriod = FindDominantPeriod(complex, cyclePeriod);

                // Calculate phase
                double phase = CalculatePhase(i % dominantPeriod, dominantPeriod);

                // Determine if we're in a typically bullish or bearish part of the cycle
                bool isBullishPhase = phase < Math.PI;

                string updateQuery = $@"
                UPDATE {tableName}
                SET CycleDominantPeriod = @DominantPeriod,
                    CyclePhase = @Phase,
                    IsBullishCyclePhase = @IsBullishPhase
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@DominantPeriod", dominantPeriod);
                    command.Parameters.AddWithValue("@Phase", phase);
                    command.Parameters.AddWithValue("@IsBullishPhase", isBullishPhase);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static Tuple<double, double> LinearTrend(double[] data)
        {
            int n = data.Length;
            double sumX = n * (n - 1) / 2;
            double sumY = data.Sum();
            double sumXY = data.Select((y, i) => i * y).Sum();
            double sumX2 = n * (n - 1) * (2 * n - 1) / 6;

            double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            double intercept = (sumY - slope * sumX) / n;

            return Tuple.Create(intercept, slope);
        }

        private static int FindDominantPeriod(System.Numerics.Complex[] fft, int n)
        {
            var magnitudes = fft.Take(n / 2).Select(c => c.Magnitude).ToArray();
            return Array.IndexOf(magnitudes, magnitudes.Max()) + 1;
        }

        private static double CalculatePhase(int position, int period)
        {
            return 2 * Math.PI * position / period;
        }
    }
}