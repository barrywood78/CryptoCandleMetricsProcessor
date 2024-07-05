using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class IchimokuIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the Ichimoku Cloud indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate Ichimoku results using the Skender.Stock.Indicators library
            var ichimokuResults = candles.GetIchimoku().Where(r => r != null).Select(r => new
            {
                r.Date,
                r.TenkanSen,
                r.KijunSen,
                r.SenkouSpanA,
                r.SenkouSpanB,
                r.ChikouSpan
            }).ToList();

            string updateQuery = $@"
                UPDATE {tableName}
                SET Ichimoku_TenkanSen = @TenkanSen,
                    Ichimoku_KijunSen = @KijunSen,
                    Ichimoku_SenkouSpanA = @SenkouSpanA,
                    Ichimoku_SenkouSpanB = @SenkouSpanB,
                    Ichimoku_ChikouSpan = @ChikouSpan
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@TenkanSen", SqliteType.Real);
            command.Parameters.Add("@KijunSen", SqliteType.Real);
            command.Parameters.Add("@SenkouSpanA", SqliteType.Real);
            command.Parameters.Add("@SenkouSpanB", SqliteType.Real);
            command.Parameters.Add("@ChikouSpan", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in ichimokuResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@TenkanSen"].Value = result.TenkanSen ?? (object)DBNull.Value;
                    command.Parameters["@KijunSen"].Value = result.KijunSen ?? (object)DBNull.Value;
                    command.Parameters["@SenkouSpanA"].Value = result.SenkouSpanA ?? (object)DBNull.Value;
                    command.Parameters["@SenkouSpanB"].Value = result.SenkouSpanB ?? (object)DBNull.Value;
                    command.Parameters["@ChikouSpan"].Value = result.ChikouSpan ?? (object)DBNull.Value;
                    command.Parameters["@StartDate"].Value = result.Date.Ticks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
