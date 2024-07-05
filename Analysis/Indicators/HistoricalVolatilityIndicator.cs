using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using MathNet.Numerics.Statistics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class HistoricalVolatilityIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            var results = new List<(long DateTicks, double HistoricalVolatility)>();
            var closePrices = candles.Select(c => (double)c.Close).ToArray();

            for (int i = period; i < candles.Count; i++)
            {
                var subset = closePrices.AsSpan().Slice(i - period, period);
                double[] returns = new double[period - 1];
                for (int j = 1; j < period; j++)
                {
                    returns[j - 1] = Math.Log(subset[j] / subset[j - 1]);
                }

                double standardDeviation = returns.StandardDeviation();
                double annualizedVolatility = standardDeviation * Math.Sqrt(252); // Assuming 252 trading days in a year

                results.Add((candles[i].Date.Ticks, annualizedVolatility));
            }

            UpdateHistoricalVolatilities(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateHistoricalVolatilities(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, double HistoricalVolatility)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET HistoricalVolatility = @HistoricalVolatility
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@HistoricalVolatility", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, historicalVolatility) in batch)
                {
                    command.Parameters["@HistoricalVolatility"].Value = historicalVolatility;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}