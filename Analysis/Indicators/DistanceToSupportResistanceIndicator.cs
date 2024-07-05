using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class DistanceToSupportResistanceIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var supportResistanceLevels = GetSupportResistanceLevels(connection, transaction, tableName, productId, granularity);
            var results = new List<(long DateTicks, decimal DistanceToSupport, decimal DistanceToResistance)>();

            foreach (var candle in candles)
            {
                if (supportResistanceLevels.TryGetValue(candle.Date.Ticks, out var levels))
                {
                    decimal closePrice = candle.Close;
                    decimal distanceToNearestSupport = (closePrice - levels.Support) / closePrice;
                    decimal distanceToNearestResistance = (levels.Resistance - closePrice) / closePrice;
                    results.Add((candle.Date.Ticks, distanceToNearestSupport, distanceToNearestResistance));
                }
            }

            UpdateDistances(connection, transaction, tableName, productId, granularity, results);
        }

        private static Dictionary<long, (decimal Support, decimal Resistance)> GetSupportResistanceLevels(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity)
        {
            var levels = new Dictionary<long, (decimal Support, decimal Resistance)>();
            string query = $@"
                SELECT StartDate, Support1, Resistance1
                FROM {tableName}
                WHERE ProductId = @ProductId AND Granularity = @Granularity";

            using var command = new SqliteCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@ProductId", productId);
            command.Parameters.AddWithValue("@Granularity", granularity);

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                long dateTicks = DateTime.Parse(reader.GetString(0)).Ticks;
                decimal support = reader.IsDBNull(1) ? 0 : reader.GetDecimal(1);
                decimal resistance = reader.IsDBNull(2) ? 0 : reader.GetDecimal(2);
                levels[dateTicks] = (support, resistance);
            }

            return levels;
        }

        private static void UpdateDistances(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal DistanceToSupport, decimal DistanceToResistance)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET DistanceToNearestSupport = @DistanceToNearestSupport,
                    DistanceToNearestResistance = @DistanceToNearestResistance
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@DistanceToNearestSupport", SqliteType.Real);
            command.Parameters.Add("@DistanceToNearestResistance", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, distanceToSupport, distanceToResistance) in batch)
                {
                    command.Parameters["@DistanceToNearestSupport"].Value = (double)distanceToSupport;
                    command.Parameters["@DistanceToNearestResistance"].Value = (double)distanceToResistance;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}