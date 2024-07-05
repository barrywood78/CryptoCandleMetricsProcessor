using Microsoft.Data.Sqlite;
using MathNet.Numerics.Statistics;
using MathNet.Numerics.LinearRegression;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using MathNet.Numerics;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class HurstExponent
    {
        private const int BatchSize = 5000;
        private static readonly int[] Lags = Enumerable.Range(2, 20).ToArray();
        private static readonly double[] LogLags = Lags.Select(l => Math.Log(l)).ToArray();

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int maxLag = 50)
        {
            var results = new ConcurrentDictionary<DateTime, double>();
            var closePrices = candles.Select(c => (double)c.Close).ToArray();

            Parallel.For(maxLag, candles.Count, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                () => new double[maxLag],
                (i, _, buffer) =>
                {
                    Array.Copy(closePrices, i - maxLag, buffer, 0, maxLag);
                    double hurst = CalculateHurst(buffer);
                    results[candles[i].Date] = hurst;
                    return buffer;
                },
                _ => { });

            UpdateDatabase(connection, transaction, tableName, productId, granularity, results);
        }

        private static double CalculateHurst(double[] prices)
        {
            var logReturns = new double[prices.Length - 1];
            for (int i = 0; i < logReturns.Length; i++)
            {
                logReturns[i] = Math.Log(prices[i + 1] / prices[i]);
            }

            var rs = new double[Lags.Length];
            Parallel.For(0, Lags.Length, i =>
            {
                rs[i] = CalculateRescaledRange(logReturns, Lags[i]);
            });

            var logRs = rs.Select(r => Math.Log(r)).ToArray();

            // Perform linear regression
            var p = Fit.Line(LogLags, logRs);

            // The Hurst exponent is the slope of the regression line
            return p.Item2;
        }

        private static double CalculateRescaledRange(double[] series, int lag)
        {
            int windowLength = series.Length - lag + 1;
            var window = new double[windowLength];
            double sum = 0;

            // Calculate initial sum
            for (int i = 0; i < lag; i++)
            {
                sum += series[i];
            }

            // Calculate moving average
            window[0] = sum / lag;

            for (int i = 1; i < windowLength; i++)
            {
                sum = sum - series[i - 1] + series[i + lag - 1];
                window[i] = sum / lag;
            }

            // Calculate deviations and cumulative deviations
            double mean = 0, variance = 0;
            double min = double.MaxValue, max = double.MinValue;
            double cumSum = 0;

            for (int i = 0; i < windowLength; i++)
            {
                double deviation = series[i] - window[i];
                mean += deviation;
                variance += deviation * deviation;
                cumSum += deviation;
                if (cumSum < min) min = cumSum;
                if (cumSum > max) max = cumSum;
            }

            mean /= windowLength;
            variance = variance / windowLength - mean * mean;

            double range = max - min;
            double stdDev = Math.Sqrt(variance);

            return range / stdDev;
        }

        private static void UpdateDatabase(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, ConcurrentDictionary<DateTime, double> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET HurstExponent = @HurstExponent
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@HurstExponent", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Text);

            foreach (var batch in results.Keys.Chunk(BatchSize))
            {
                foreach (var date in batch)
                {
                    command.Parameters["@HurstExponent"].Value = results[date];
                    command.Parameters["@StartDate"].Value = date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}