using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RSIChangeIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var rsiResults = candles.GetRsi(period).ToList();
            for (int i = period; i < candles.Count; i++)
            {
                if (rsiResults[i].Rsi.HasValue && rsiResults[i - 1].Rsi.HasValue)
                {
                    decimal rsiChange = (decimal)(rsiResults[i].Rsi ?? 0) - (decimal)(rsiResults[i - 1].Rsi ?? 0);
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET RSIChange = @RSIChange
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";
                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@RSIChange", rsiChange);
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