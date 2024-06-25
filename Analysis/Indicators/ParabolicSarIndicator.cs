using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ParabolicSarIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var parabolicSarResults = candles.GetParabolicSar().ToList();

            for (int i = 0; i < parabolicSarResults.Count; i++)
            {
                if (parabolicSarResults[i].Sar != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET ParabolicSar = @ParabolicSar
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@ParabolicSar", parabolicSarResults[i].Sar);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", parabolicSarResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
