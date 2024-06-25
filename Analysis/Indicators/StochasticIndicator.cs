using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class StochasticIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var stochasticResults = candles.GetStoch(period).ToList();

            for (int i = 0; i < stochasticResults.Count; i++)
            {
                if (stochasticResults[i].K != null && stochasticResults[i].D != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET Stoch_K = @Stoch_K, Stoch_D = @Stoch_D
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@Stoch_K", stochasticResults[i].K);
                        command.Parameters.AddWithValue("@Stoch_D", stochasticResults[i].D);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", stochasticResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
