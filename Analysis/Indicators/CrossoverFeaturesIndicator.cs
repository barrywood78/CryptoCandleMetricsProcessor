using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CrossoverFeaturesIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            const int batchSize = 5000;

            var macdResults = candles.GetMacd().ToList();
            var emaShortResults = candles.GetEma(12).ToList();
            var emaLongResults = candles.GetEma(26).ToList();

            var crossovers = new ConcurrentBag<(DateTime Date, bool MACDCrossover, bool EMACrossover)>();

            // Parallel calculation of crossovers
            Parallel.For(1, candles.Count, i =>
            {
                bool macdCrossover = (macdResults[i].Macd > macdResults[i].Signal && macdResults[i - 1].Macd <= macdResults[i - 1].Signal) ||
                                     (macdResults[i].Macd < macdResults[i].Signal && macdResults[i - 1].Macd >= macdResults[i - 1].Signal);
                bool emaCrossover = (emaShortResults[i].Ema > emaLongResults[i].Ema && emaShortResults[i - 1].Ema <= emaLongResults[i - 1].Ema) ||
                                    (emaShortResults[i].Ema < emaLongResults[i].Ema && emaShortResults[i - 1].Ema >= emaLongResults[i - 1].Ema);

                crossovers.Add((candles[i].Date, macdCrossover, emaCrossover));
            });

            // Sort crossovers by date for consistent updating
            var sortedCrossovers = crossovers.OrderBy(c => c.Date).ToList();

            // Prepare the SQL command
            string updateQuery = $@"
                UPDATE {tableName}
                SET MACDCrossover = @MACDCrossover,
                    EMACrossover = @EMACrossover
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@MACDCrossover", SqliteType.Integer);
            command.Parameters.Add("@EMACrossover", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Text);

            // Batch update
            for (int i = 0; i < sortedCrossovers.Count; i += batchSize)
            {
                int endIdx = Math.Min(i + batchSize, sortedCrossovers.Count);
                for (int j = i; j < endIdx; j++)
                {
                    var crossover = sortedCrossovers[j];
                    command.Parameters["@MACDCrossover"].Value = crossover.MACDCrossover ? 1 : 0;
                    command.Parameters["@EMACrossover"].Value = crossover.EMACrossover ? 1 : 0;
                    command.Parameters["@StartDate"].Value = crossover.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}