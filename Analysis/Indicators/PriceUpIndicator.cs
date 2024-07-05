﻿using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceUpIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates whether the price is up (1) or not (0) compared to the previous period and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var priceUpResults = new List<(long DateTicks, int PriceUp)>();

            // Iterate through each candle starting from the second one
            for (int i = 1; i < candles.Count; i++)
            {
                // Determine if the price is up compared to the previous candle
                int priceUp = candles[i].Close > candles[i - 1].Close ? 1 : 0;
                priceUpResults.Add((candles[i].Date.Ticks, priceUp));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET PriceUp = @PriceUp
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@PriceUp", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in priceUpResults
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@PriceUp"].Value = result.PriceUp;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
