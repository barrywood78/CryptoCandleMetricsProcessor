using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

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

            Parallel.For(maxLag, candles.Count, i =>
            {
                var prices = closePrices.AsSpan().Slice(i - maxLag, maxLag);
                double hurst = CalculateHurst(prices);
                results[candles[i].Date] = hurst;
            });

            UpdateDatabase(connection, transaction, tableName, productId, granularity, results);
        }

        private static double CalculateHurst(ReadOnlySpan<double> prices)
        {
            Span<double> rs = stackalloc double[Lags.Length];
            Span<double> values = stackalloc double[prices.Length - Lags.Max()]; // Allocate enough space for the largest lag

            for (int lagIndex = 0; lagIndex < Lags.Length; lagIndex++)
            {
                int lag = Lags[lagIndex];
                int valuesLength = prices.Length - lag;

                // Reuse the same span, only use the necessary portion
                var valuesSlice = values.Slice(0, valuesLength);

                for (int i = 0; i < valuesLength; i++)
                {
                    valuesSlice[i] = Math.Log(prices[i + lag] / prices[i]);
                }

                double mean = 0;
                for (int i = 0; i < valuesLength; i++) mean += valuesSlice[i];
                mean /= valuesLength;

                double sumSquaredDiff = 0;
                double min = double.MaxValue, max = double.MinValue;
                double cumSum = 0;

                for (int i = 0; i < valuesLength; i++)
                {
                    double adjustedValue = valuesSlice[i] - mean;
                    sumSquaredDiff += adjustedValue * adjustedValue;
                    cumSum += adjustedValue;
                    if (cumSum < min) min = cumSum;
                    if (cumSum > max) max = cumSum;
                }

                double range = max - min;
                double stdDev = Math.Sqrt(sumSquaredDiff / valuesLength);
                rs[lagIndex] = range / stdDev;
            }

            double sumXY = 0, sumX = 0, sumY = 0, sumX2 = 0;
            for (int i = 0; i < rs.Length; i++)
            {
                double logRs = Math.Log(rs[i]);
                sumXY += LogLags[i] * logRs;
                sumX += LogLags[i];
                sumY += logRs;
                sumX2 += LogLags[i] * LogLags[i];
            }

            int n = rs.Length;
            return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
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
