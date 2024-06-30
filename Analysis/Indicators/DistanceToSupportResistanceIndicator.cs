using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class DistanceToSupportResistanceIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            for (int i = 0; i < candles.Count; i++)
            {
                // Retrieve support and resistance levels from the database
                decimal support1 = GetSupportResistanceLevel(connection, tableName, productId, granularity, candles[i].Date, "Support1");
                decimal resistance1 = GetSupportResistanceLevel(connection, tableName, productId, granularity, candles[i].Date, "Resistance1");

                decimal closePrice = candles[i].Close;
                decimal distanceToNearestSupport = (closePrice - support1) / closePrice;
                decimal distanceToNearestResistance = (resistance1 - closePrice) / closePrice;

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET DistanceToNearestSupport = @DistanceToNearestSupport,
                        DistanceToNearestResistance = @DistanceToNearestResistance
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@DistanceToNearestSupport", distanceToNearestSupport);
                    command.Parameters.AddWithValue("@DistanceToNearestResistance", distanceToNearestResistance);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static decimal GetSupportResistanceLevel(SqliteConnection connection, string tableName, string productId, string granularity, DateTime date, string columnName)
        {
            string query = $@"
                SELECT {columnName}
                FROM {tableName}
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

            using (var command = new SqliteCommand(query, connection))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);
                command.Parameters.AddWithValue("@StartDate", date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                var result = command.ExecuteScalar();
                return result != null ? Convert.ToDecimal(result) : 0;
            }
        }
    }
}