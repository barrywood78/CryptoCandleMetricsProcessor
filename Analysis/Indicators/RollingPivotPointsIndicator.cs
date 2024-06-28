using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RollingPivotPointsIndicator
    {
        /// <summary>
        /// Calculates the rolling pivot points indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="windowPeriods">The number of periods in the rolling window.</param>
        /// <param name="offsetPeriods">The number of offset periods for the rolling window.</param>
        /// <param name="pointType">The type of pivot point to calculate (default is Standard).</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int windowPeriods, int offsetPeriods, PivotPointType pointType = PivotPointType.Standard)
        {
            // Calculate rolling pivot points results using the Skender.Stock.Indicators library
            var rollingPivotPointsResults = candles.GetRollingPivots(windowPeriods, offsetPeriods, pointType).ToList();

            // Iterate through each rolling pivot point result and update the database
            for (int i = 0; i < rollingPivotPointsResults.Count; i++)
            {
                if (rollingPivotPointsResults[i].PP != null) // Only update if Pivot Point value is not null
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
                        // Add parameters to the update command
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

                        // Execute the update command
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
