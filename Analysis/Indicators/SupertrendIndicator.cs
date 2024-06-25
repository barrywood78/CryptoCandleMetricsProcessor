using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class SupertrendIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int atrPeriod = 14, decimal multiplier = 3.0m)
        {
            var supertrendResults = candles.GetSuperTrend(atrPeriod, (double)multiplier).ToList();

            for (int i = 0; i < supertrendResults.Count; i++)
            {
                if (supertrendResults[i].SuperTrend != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET SuperTrend = @SuperTrend
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@SuperTrend", supertrendResults[i].SuperTrend);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", supertrendResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
