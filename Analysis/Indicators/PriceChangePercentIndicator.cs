using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceChangePercentIndicator
    {
        private const int BatchSize = 50000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var priceChangeResults = new List<(long DateTicks, decimal PriceChangePercent)>();

            for (int i = 1; i < candles.Count; i++)
            {
                decimal priceChangePercent = (candles[i].Close - candles[i].Open) / candles[i].Open;
                priceChangeResults.Add((candles[i].Date.Ticks, priceChangePercent));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET PriceChangePercent = @PriceChangePercent
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@PriceChangePercent", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in priceChangeResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@PriceChangePercent"].Value = result.PriceChangePercent;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
