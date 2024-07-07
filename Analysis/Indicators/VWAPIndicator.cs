using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class VWAPIndicator
    {
        private const int BatchSize = 50000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var vwapResults = candles.GetVwap().Where(r => r.Vwap.HasValue).Select(r => new { r.Date, r.Vwap }).ToList();

            var vwapData = new List<(long DateTicks, decimal Vwap)>();

            // Prepare results for batch update
            foreach (var result in vwapResults)
            {
                vwapData.Add((result.Date.Ticks, (decimal)result.Vwap!));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET VWAP = @VWAP
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@VWAP", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in vwapData
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@VWAP"].Value = result.Vwap;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
