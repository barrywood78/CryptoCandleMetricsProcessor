using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MacdIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the Moving Average Convergence Divergence (MACD) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate MACD results using the Skender.Stock.Indicators library
            var macdResults = candles.GetMacd()
                                     .Where(r => r.Macd.HasValue && r.Signal.HasValue && r.Histogram.HasValue)
                                     .Select(r => new { r.Date, r.Macd, r.Signal, r.Histogram })
                                     .ToList();

            string updateQuery = $@"
                UPDATE {tableName}
                SET MACD = @MACD, MACD_Signal = @MACD_Signal, MACD_Histogram = @MACD_Histogram
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@MACD", SqliteType.Real);
            command.Parameters.Add("@MACD_Signal", SqliteType.Real);
            command.Parameters.Add("@MACD_Histogram", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in macdResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@MACD"].Value = result.Macd;
                    command.Parameters["@MACD_Signal"].Value = result.Signal;
                    command.Parameters["@MACD_Histogram"].Value = result.Histogram;
                    command.Parameters["@StartDate"].Value = result.Date.Ticks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
