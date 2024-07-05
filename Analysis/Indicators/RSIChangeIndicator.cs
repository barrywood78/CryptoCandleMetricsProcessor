using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RSIChangeIndicator
    {
        private const int BatchSize = 50000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var rsiResults = candles.GetRsi(period).ToList();

            var rsiChangeResults = new List<(long DateTicks, decimal RSIChange)>();

            for (int i = period; i < candles.Count; i++)
            {
                if (rsiResults[i].Rsi.HasValue && rsiResults[i - 1].Rsi.HasValue)
                {
                    decimal rsiChange = (decimal)(rsiResults[i].Rsi ?? 0) - (decimal)(rsiResults[i - 1].Rsi ?? 0);
                    rsiChangeResults.Add((candles[i].Date.Ticks, rsiChange));
                }
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET RSIChange = @RSIChange
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@RSIChange", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in rsiChangeResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@RSIChange"].Value = result.RSIChange;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
