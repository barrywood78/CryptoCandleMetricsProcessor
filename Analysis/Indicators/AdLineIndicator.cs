﻿using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class AdLineIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var adLineResults = candles.GetAdl().ToList();

            for (int i = 0; i < adLineResults.Count; i++)
            {
                string updateQuery = $@"
                    UPDATE {tableName}
                    SET ADL = @ADL
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@ADL", adLineResults[i].Adl);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", adLineResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
