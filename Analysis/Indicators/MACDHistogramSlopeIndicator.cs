using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MACDHistogramSlopeIndicator
    {
        private const int BatchSize = 50000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var macdResults = candles.GetMacd().ToList();

            // Prepare results for batch update
            var results = new List<(long DateTicks, decimal MacdHistogramSlope)>();

            for (int i = 1; i < macdResults.Count; i++)
            {
                if (macdResults[i].Histogram.HasValue && macdResults[i - 1].Histogram.HasValue)
                {
                    decimal macdHistogramSlope = (decimal)(macdResults[i].Histogram ?? 0) - (decimal)(macdResults[i - 1].Histogram ?? 0);
                    results.Add((candles[i].Date.Ticks, macdHistogramSlope));
                }
            }

            // Update database in batches
            string updateQuery = $@"
                UPDATE {tableName}
                SET MACDHistogramSlope = @MACDHistogramSlope
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@MACDHistogramSlope", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@MACDHistogramSlope"].Value = result.MacdHistogramSlope;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
