using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ROCIndicator
    {
        private const int BatchSize = 50000;

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var roc5Results = candles.GetRoc(5).ToList();
            var roc10Results = candles.GetRoc(10).ToList();

            var rocResults = new List<(long DateTicks, decimal? Roc5, decimal? Roc10)>();

            for (int i = 0; i < candles.Count; i++)
            {
                decimal? roc5 = roc5Results[i].Roc.HasValue ? (decimal?)roc5Results[i].Roc : null;
                decimal? roc10 = roc10Results[i].Roc.HasValue ? (decimal?)roc10Results[i].Roc : null;
                rocResults.Add((candles[i].Date.Ticks, roc5, roc10));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET ROC_5 = @ROC5, ROC_10 = @ROC10
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ROC5", SqliteType.Real);
            command.Parameters.Add("@ROC10", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in rocResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@ROC5"].Value = result.Roc5.HasValue ? (object)result.Roc5.Value : DBNull.Value;
                    command.Parameters["@ROC10"].Value = result.Roc10.HasValue ? (object)result.Roc10.Value : DBNull.Value;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
