using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class ROCIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var roc5Results = candles.GetRoc(5).ToList();
            var roc10Results = candles.GetRoc(10).ToList();

            for (int i = 0; i < candles.Count; i++)
            {
                string updateQuery = $@"
                    UPDATE {tableName}
                    SET ROC_5 = @ROC5, ROC_10 = @ROC10
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    // Handle ROC_5
                    double? roc5 = roc5Results[i].Roc;
                    command.Parameters.AddWithValue("@ROC5", roc5.HasValue ? Convert.ToDecimal(roc5.Value) : (object)DBNull.Value);

                    // Handle ROC_10
                    double? roc10 = roc10Results[i].Roc;
                    command.Parameters.AddWithValue("@ROC10", roc10.HasValue ? Convert.ToDecimal(roc10.Value) : (object)DBNull.Value);

                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}