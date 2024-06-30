using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ATRPercentIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var atrResults = candles.GetAtr(period).ToList();

            for (int i = period - 1; i < candles.Count; i++)
            {
                if (atrResults[i].Atr.HasValue)
                {
                    decimal atrPercent = (decimal)(atrResults[i].Atr ?? 0) / candles[i].Close;


                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET ATRPercent = @ATRPercent
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@ATRPercent", atrPercent);
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