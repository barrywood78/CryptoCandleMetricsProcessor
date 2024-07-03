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
                var window = closePrices.AsSpan().Slice(i - period + 1, period).ToArray();
                var stats = CalculateStatistics(window);
                lock (results)
                {
                    results[dates[i]] = stats;
                }
            });

            UpdateDatabase(connection, transaction, tableName, productId, granularity, results);
        }

        private static (double? Mean, double? StdDev, double? Variance, double? Skewness, double? Kurtosis) CalculateStatistics(double[] data)
        {
            if (data.Length == 0)
                return (null, null, null, null, null);

            try
            {
                double mean = data.Mean();
                double stdDev = data.StandardDeviation();
                double variance = data.Variance();
                double skewness = data.Skewness();
                double kurtosis = data.Kurtosis();

                return (
                    double.IsNaN(mean) || double.IsInfinity(mean) ? (double?)null : mean,
                    double.IsNaN(stdDev) || double.IsInfinity(stdDev) ? (double?)null : stdDev,
                    double.IsNaN(variance) || double.IsInfinity(variance) ? (double?)null : variance,
                    double.IsNaN(skewness) || double.IsInfinity(skewness) ? (double?)null : skewness,
                    double.IsNaN(kurtosis) || double.IsInfinity(kurtosis) ? (double?)null : kurtosis
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in statistical calculation: {ex.Message}");
                return (null, null, null, null, null);
            }
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
                    command.Parameters["@Mean"].Value = mean.HasValue ? (object)mean.Value : DBNull.Value;
                    command.Parameters["@StdDev"].Value = stdDev.HasValue ? (object)stdDev.Value : DBNull.Value;
                    command.Parameters["@Variance"].Value = variance.HasValue ? (object)variance.Value : DBNull.Value;
                    command.Parameters["@Skewness"].Value = skewness.HasValue ? (object)skewness.Value : DBNull.Value;
                    command.Parameters["@Kurtosis"].Value = kurtosis.HasValue ? (object)kurtosis.Value : DBNull.Value;
                    command.Parameters["@StartDate"].Value = date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}