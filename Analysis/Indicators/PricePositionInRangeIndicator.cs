using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PricePositionInRangeIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var results = candles.Select(candle =>
            {
                decimal pricePositionInRange = (candle.High != candle.Low)
                    ? (candle.Close - candle.Low) / (candle.High - candle.Low)
                    : 0.5m; // Assuming mid-range when high equals low
                return (DateTicks: candle.Date.Ticks, PricePositionInRange: pricePositionInRange);
            }).ToList();

            UpdatePricePositions(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdatePricePositions(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal PricePositionInRange)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET PricePositionInRange = @PricePositionInRange
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@PricePositionInRange", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, pricePositionInRange) in batch)
                {
                    command.Parameters["@PricePositionInRange"].Value = (double)pricePositionInRange;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}