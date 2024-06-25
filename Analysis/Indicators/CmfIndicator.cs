﻿using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CmfIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            var cmfResults = candles.GetCmf(period).ToList();

            for (int i = 0; i < cmfResults.Count; i++)
            {
                if (cmfResults[i].Cmf != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET CMF = @CMF
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@CMF", cmfResults[i].Cmf);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", cmfResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
