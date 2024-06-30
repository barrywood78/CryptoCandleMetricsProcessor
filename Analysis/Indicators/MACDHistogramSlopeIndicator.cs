using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MACDHistogramSlopeIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var macdResults = candles.GetMacd().ToList();
            for (int i = 1; i < macdResults.Count; i++)
            {
                if (macdResults[i].Histogram.HasValue && macdResults[i - 1].Histogram.HasValue)
                {
                    decimal macdHistogramSlope = (decimal)(macdResults[i].Histogram ?? 0) - (decimal)(macdResults[i - 1].Histogram ?? 0);
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET MACDHistogramSlope = @MACDHistogramSlope
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";
                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@MACDHistogramSlope", macdHistogramSlope);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}