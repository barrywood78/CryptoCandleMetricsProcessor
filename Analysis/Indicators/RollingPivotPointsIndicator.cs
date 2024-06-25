using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RollingPivotPointsIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int windowPeriods, int offsetPeriods, PivotPointType pointType = PivotPointType.Standard)
        {
            var rollingPivotPointsResults = candles.GetRollingPivots(windowPeriods, offsetPeriods, pointType).ToList();

            for (int i = 0; i < rollingPivotPointsResults.Count; i++)
            {
                if (rollingPivotPointsResults[i].PP != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET PivotPoint = @Pivot,
                            Resistance1 = @Resistance1,
                            Resistance2 = @Resistance2,
                            Resistance3 = @Resistance3,
                            Support1 = @Support1,
                            Support2 = @Support2,
                            Support3 = @Support3
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@Pivot", rollingPivotPointsResults[i].PP);
                        command.Parameters.AddWithValue("@Resistance1", rollingPivotPointsResults[i].R1);
                        command.Parameters.AddWithValue("@Resistance2", rollingPivotPointsResults[i].R2);
                        command.Parameters.AddWithValue("@Resistance3", rollingPivotPointsResults[i].R3);
                        command.Parameters.AddWithValue("@Support1", rollingPivotPointsResults[i].S1);
                        command.Parameters.AddWithValue("@Support2", rollingPivotPointsResults[i].S2);
                        command.Parameters.AddWithValue("@Support3", rollingPivotPointsResults[i].S3);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", rollingPivotPointsResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
