using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class EmaIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var emaResults = candles.GetEma(period).ToList();

            for (int i = 0; i < emaResults.Count; i++)
            {
                if (emaResults[i].Ema != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET EMA = @EMA
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@EMA", emaResults[i].Ema);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", emaResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
