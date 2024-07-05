using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class VolumeChangePercentIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var results = new List<(long DateTicks, decimal VolumeChangePercent)>();

            for (int i = 1; i < candles.Count; i++)
            {
                decimal volumeChangePercent = (candles[i].Volume - candles[i - 1].Volume) / candles[i - 1].Volume;
                results.Add((candles[i].Date.Ticks, volumeChangePercent));
            }

            UpdateVolumeChangePercents(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateVolumeChangePercents(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal VolumeChangePercent)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET VolumeChangePercent = @VolumeChangePercent
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@VolumeChangePercent", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, volumeChangePercent) in batch)
                {
                    command.Parameters["@VolumeChangePercent"].Value = (double)volumeChangePercent;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}