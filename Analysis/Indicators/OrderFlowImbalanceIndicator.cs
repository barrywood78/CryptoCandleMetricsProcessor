using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class OrderFlowImbalanceIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var results = new List<(long DateTicks, decimal Imbalance)>();

            for (int i = period - 1; i < candles.Count; i++)
            {
                var periodCandles = candles.Skip(i - period + 1).Take(period);
                decimal imbalance = CalculateImbalance(periodCandles);
                results.Add((candles[i].Date.Ticks, imbalance));
            }

            UpdateOrderFlowImbalances(connection, transaction, tableName, productId, granularity, results);
        }

        private static decimal CalculateImbalance(IEnumerable<Quote> periodCandles)
        {
            decimal buyingPressure = 0;
            decimal sellingPressure = 0;

            foreach (var candle in periodCandles)
            {
                if (candle.Close > candle.Open)
                {
                    buyingPressure += candle.Volume * (candle.Close - candle.Open) / candle.Open;
                }
                else if (candle.Close < candle.Open)
                {
                    sellingPressure += candle.Volume * (candle.Open - candle.Close) / candle.Open;
                }
            }

            return (buyingPressure - sellingPressure) / (buyingPressure + sellingPressure);
        }

        private static void UpdateOrderFlowImbalances(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal Imbalance)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET OrderFlowImbalance = @OrderFlowImbalance
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@OrderFlowImbalance", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, imbalance) in batch)
                {
                    command.Parameters["@OrderFlowImbalance"].Value = (double)imbalance;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}