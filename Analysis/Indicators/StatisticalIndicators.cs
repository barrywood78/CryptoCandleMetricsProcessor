using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using MathNet.Numerics.Statistics;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class StatisticalIndicators
    {
        private const int BatchSize = 5000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var closePrices = candles.Select(c => (double)c.Close).ToArray();
            var dates = candles.Select(c => c.Date).ToArray();

            var results = new Dictionary<DateTime, (double? Mean, double? StdDev, double? Variance, double? Skewness, double? Kurtosis)>();

            Parallel.For(period - 1, closePrices.Length, i =>
            {
                var window = closePrices.AsSpan().Slice(i - period + 1, period);
                var stats = CalculateStatistics(window);
                lock (results)
                {
                    results[dates[i]] = stats;
                }
            });

            UpdateDatabase(connection, transaction, tableName, productId, granularity, results);
        }

        private static (double Mean, double StdDev, double Variance, double Skewness, double Kurtosis) CalculateStatistics(Span<double> data)
        {
            double sum = 0, sumSquared = 0, sumCubed = 0, sumFourth = 0;
            for (int i = 0; i < data.Length; i++)
            {
                double x = data[i];
                sum += x;
                double x2 = x * x;
                sumSquared += x2;
                sumCubed += x2 * x;
                sumFourth += x2 * x2;
            }

            double mean = sum / data.Length;
            double variance = (sumSquared - sum * sum / data.Length) / (data.Length - 1);
            double stdDev = Math.Sqrt(variance);

            double m2 = sumSquared / data.Length - mean * mean;
            double m3 = (sumCubed - 3 * mean * sumSquared + 2 * mean * mean * sum) / data.Length;
            double m4 = (sumFourth - 4 * mean * sumCubed + 6 * mean * mean * sumSquared - 3 * mean * mean * mean * sum) / data.Length;

            double skewness = m3 / (stdDev * stdDev * stdDev);
            double kurtosis = m4 / (m2 * m2) - 3;

            return (mean, stdDev, variance, skewness, kurtosis);
        }

        private static void UpdateDatabase(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, Dictionary<DateTime, (double? Mean, double? StdDev, double? Variance, double? Skewness, double? Kurtosis)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET RollingMean = @Mean,
                    RollingStdDev = @StdDev,
                    RollingVariance = @Variance,
                    RollingSkewness = @Skewness,
                    RollingKurtosis = @Kurtosis
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@Mean", SqliteType.Real);
            command.Parameters.Add("@StdDev", SqliteType.Real);
            command.Parameters.Add("@Variance", SqliteType.Real);
            command.Parameters.Add("@Skewness", SqliteType.Real);
            command.Parameters.Add("@Kurtosis", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Text);

            foreach (var batch in results.Keys.Chunk(BatchSize))
            {
                foreach (var date in batch)
                {
                    var (mean, stdDev, variance, skewness, kurtosis) = results[date];
                    command.Parameters["@Mean"].Value = mean.HasValue && !double.IsNaN(mean.Value) ? (object)mean.Value : DBNull.Value;
                    command.Parameters["@StdDev"].Value = stdDev.HasValue && !double.IsNaN(stdDev.Value) ? (object)stdDev.Value : DBNull.Value;
                    command.Parameters["@Variance"].Value = variance.HasValue && !double.IsNaN(variance.Value) ? (object)variance.Value : DBNull.Value;
                    command.Parameters["@Skewness"].Value = skewness.HasValue && !double.IsNaN(skewness.Value) ? (object)skewness.Value : DBNull.Value;
                    command.Parameters["@Kurtosis"].Value = kurtosis.HasValue && !double.IsNaN(kurtosis.Value) ? (object)kurtosis.Value : DBNull.Value;
                    command.Parameters["@StartDate"].Value = date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}