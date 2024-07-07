using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearRegression;
using MathNet.Numerics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class FractalDimensionIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int window = 30)
        {
            var results = new List<(long DateTicks, double FractalDimension)>();

            for (int i = window; i < candles.Count; i++)
            {
                var prices = candles.Skip(i - window).Take(window).Select(c => (double)c.Close).ToArray();
                double fractalDimension = CalculateHiguchiFractalDimension(prices);
                results.Add((candles[i].Date.Ticks, fractalDimension));
            }

            UpdateFractalDimensions(connection, transaction, tableName, productId, granularity, results);
        }

        private static double CalculateHiguchiFractalDimension(double[] data, int kmax = 10)
        {
            var lnLengths = new List<double>();
            var lnKs = new List<double>();
            for (int k = 1; k <= kmax; k++)
            {
                double length = Enumerable.Range(0, k)
                    .AsParallel()
                    .Select(m =>
                    {
                        return Enumerable.Range(0, (data.Length - m - 1) / k)
                            .Sum(i => Math.Abs(data[m + (i + 1) * k] - data[m + i * k]));
                    })
                    .Sum();

                if (length > 0)
                {
                    length *= (data.Length - 1) / (Math.Pow(k, 2) * ((data.Length - 1) / k));
                    lnLengths.Add(Math.Log(length));
                    lnKs.Add(Math.Log(1.0 / k));
                }
            }

            // Check if we have enough data points for regression
            if (lnLengths.Count < 2)
            {
                return 0; // or another appropriate default value
            }

            // Use MathNet.Numerics for linear regression
            double[] x = lnKs.ToArray();
            double[] y = lnLengths.ToArray();
            var p = Fit.Line(x, y);
            return double.IsNaN(p.Item2) || double.IsInfinity(p.Item2) ? 0 : p.Item2; // Return the slope or 0 if it's NaN or Infinity
        }

        private static void UpdateFractalDimensions(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, double FractalDimension)> results)
        {
            string updateQuery = $@"
        UPDATE {tableName}
        SET FractalDimension = @FractalDimension
        WHERE ProductId = @ProductId
          AND Granularity = @Granularity
          AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";
            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@FractalDimension", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);
            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, fractalDimension) in batch)
                {
                    if (!double.IsNaN(fractalDimension) && !double.IsInfinity(fractalDimension))
                    {
                        command.Parameters["@FractalDimension"].Value = fractalDimension;
                        command.Parameters["@StartDate"].Value = dateTicks;
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}