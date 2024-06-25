using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceUpIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            for (int i = 1; i < candles.Count; i++)
            {
                int priceUp = candles[i].Close > candles[i - 1].Close ? 1 : 0;

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET PriceUp = @PriceUp
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@PriceUp", priceUp);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
