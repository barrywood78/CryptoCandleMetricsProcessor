using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class RollingPivotPointsIndicator
    {
        private const int BatchSize = 50000;

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

            var pivotPointsResults = new List<(long DateTicks, decimal? PP, decimal? R1, decimal? R2, decimal? R3, decimal? S1, decimal? S2, decimal? S3)>();

            // Prepare results for batch update
            for (int i = 0; i < rollingPivotPointsResults.Count; i++)
            {
                if (rollingPivotPointsResults[i].PP != null) // Only update if Pivot Point value is not null
                {
                    pivotPointsResults.Add((
                        rollingPivotPointsResults[i].Date.Ticks,
                        rollingPivotPointsResults[i].PP,
                        rollingPivotPointsResults[i].R1,
                        rollingPivotPointsResults[i].R2,
                        rollingPivotPointsResults[i].R3,
                        rollingPivotPointsResults[i].S1,
                        rollingPivotPointsResults[i].S2,
                        rollingPivotPointsResults[i].S3
                    ));
                }
            }

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
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@Pivot", SqliteType.Real);
            command.Parameters.Add("@Resistance1", SqliteType.Real);
            command.Parameters.Add("@Resistance2", SqliteType.Real);
            command.Parameters.Add("@Resistance3", SqliteType.Real);
            command.Parameters.Add("@Support1", SqliteType.Real);
            command.Parameters.Add("@Support2", SqliteType.Real);
            command.Parameters.Add("@Support3", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in pivotPointsResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@Pivot"].Value = result.PP ?? (object)DBNull.Value;
                    command.Parameters["@Resistance1"].Value = result.R1 ?? (object)DBNull.Value;
                    command.Parameters["@Resistance2"].Value = result.R2 ?? (object)DBNull.Value;
                    command.Parameters["@Resistance3"].Value = result.R3 ?? (object)DBNull.Value;
                    command.Parameters["@Support1"].Value = result.S1 ?? (object)DBNull.Value;
                    command.Parameters["@Support2"].Value = result.S2 ?? (object)DBNull.Value;
                    command.Parameters["@Support3"].Value = result.S3 ?? (object)DBNull.Value;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
