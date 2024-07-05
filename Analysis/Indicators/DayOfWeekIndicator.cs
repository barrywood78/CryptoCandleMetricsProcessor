using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class DayOfWeekIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var dayOfWeekResults = candles.Select(candle => new
            {
                candle.Date,
                DayOfWeek = ((int)candle.Date.DayOfWeek == 0) ? 7 : (int)candle.Date.DayOfWeek
            }).ToList();

            string updateQuery = $@"
                UPDATE {tableName}
                SET DayOfWeek = @DayOfWeek
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@DayOfWeek", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in dayOfWeekResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@DayOfWeek"].Value = result.DayOfWeek;
                    command.Parameters["@StartDate"].Value = result.Date.Ticks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
