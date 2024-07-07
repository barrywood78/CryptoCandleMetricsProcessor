using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class StochasticIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the Stochastic Oscillator indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the Stochastic Oscillator calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate Stochastic Oscillator results using the Skender.Stock.Indicators library
            var stochasticResults = candles.GetStoch(period).Where(r => r.K.HasValue && r.D.HasValue).Select(r => new { r.Date, r.K, r.D }).ToList();

            var stochData = new List<(long DateTicks, decimal Stoch_K, decimal Stoch_D)>();

            // Prepare results for batch update
            foreach (var result in stochasticResults)
            {
                stochData.Add((result.Date.Ticks, (decimal)result.K!, (decimal)result.D!));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET Stoch_K = @Stoch_K, Stoch_D = @Stoch_D
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@Stoch_K", SqliteType.Real);
            command.Parameters.Add("@Stoch_D", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in stochData
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@Stoch_K"].Value = result.Stoch_K;
                    command.Parameters["@Stoch_D"].Value = result.Stoch_D;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
