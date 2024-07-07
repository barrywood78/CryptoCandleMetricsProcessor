using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class SupertrendIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the SuperTrend indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="atrPeriod">The period to use for the Average True Range (ATR) calculation (default is 14).</param>
        /// <param name="multiplier">The multiplier to use for the SuperTrend calculation (default is 3.0).</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int atrPeriod = 14, decimal multiplier = 3.0m)
        {
            // Calculate SuperTrend results using the Skender.Stock.Indicators library
            var supertrendResults = candles.GetSuperTrend(atrPeriod, (double)multiplier).Where(r => r.SuperTrend.HasValue).Select(r => new { r.Date, r.SuperTrend }).ToList();

            var supertrendData = new List<(long DateTicks, decimal SuperTrend)>();

            // Prepare results for batch update
            foreach (var result in supertrendResults)
            {
                supertrendData.Add((result.Date.Ticks, (decimal)result.SuperTrend!));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET SuperTrend = @SuperTrend
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@SuperTrend", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in supertrendData
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@SuperTrend"].Value = result.SuperTrend;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
