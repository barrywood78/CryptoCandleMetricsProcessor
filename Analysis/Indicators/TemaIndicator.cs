﻿using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class TemaIndicator
    {
        private const int BatchSize = 50000;

        /// <summary>
        /// Calculates the Triple Exponential Moving Average (TEMA) indicator and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the TEMA calculation.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate TEMA results using the Skender.Stock.Indicators library
            var temaResults = candles.GetTema(period).Where(r => r.Tema.HasValue).Select(r => new { r.Date, r.Tema }).ToList();

            var temaData = new List<(long DateTicks, decimal Tema)>();

            // Prepare results for batch update
            foreach (var result in temaResults)
            {
                temaData.Add((result.Date.Ticks, (decimal)result.Tema!));
            }

            string updateQuery = $@"
                UPDATE {tableName}
                SET TEMA = @TEMA
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@TEMA", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in temaData
                .Select((value, index) => new { value, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(group => group.Select(x => x.value).ToList()))
            {
                foreach (var result in batch)
                {
                    command.Parameters["@TEMA"].Value = result.Tema;
                    command.Parameters["@StartDate"].Value = result.DateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
