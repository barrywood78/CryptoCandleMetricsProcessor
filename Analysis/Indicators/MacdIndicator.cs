using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MacdIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var macdResults = candles.GetMacd().ToList();

            for (int i = 0; i < macdResults.Count; i++)
            {
                if (macdResults[i].Macd != null && macdResults[i].Signal != null && macdResults[i].Histogram != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET MACD = @MACD, MACD_Signal = @MACD_Signal, MACD_Histogram = @MACD_Histogram
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@MACD", macdResults[i].Macd);
                        command.Parameters.AddWithValue("@MACD_Signal", macdResults[i].Signal);
                        command.Parameters.AddWithValue("@MACD_Histogram", macdResults[i].Histogram);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", macdResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
