using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CompositeMarketSentimentIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var indicators = new[] { "RSI", "MACD_Histogram", "BB_PercentB", "ADX", "RelativeVolume" };
            var indicatorValues = GetIndicatorValues(connection, transaction, tableName, productId, granularity, indicators);

            var sentiments = new List<(long DateTicks, decimal Sentiment, string Category)>();

            for (int i = 0; i < candles.Count; i++)
            {
                decimal sentiment = 0;
                int componentsCount = 0;

                if (indicatorValues["RSI"][i].HasValue)
                {
                    sentiment += ((indicatorValues["RSI"][i] ?? 0) - 50) / 50;
                    componentsCount++;
                }

                if (indicatorValues["MACD_Histogram"][i].HasValue)
                {
                    sentiment += (indicatorValues["MACD_Histogram"][i] ?? 0) / candles[i].Close;
                    componentsCount++;
                }

                if (indicatorValues["BB_PercentB"][i].HasValue)
                {
                    sentiment += (indicatorValues["BB_PercentB"][i] ?? 0) - 0.5m;
                    componentsCount++;
                }

                if (indicatorValues["ADX"][i].HasValue)
                {
                    sentiment += ((indicatorValues["ADX"][i] ?? 0) / 100) - 0.5m;
                    componentsCount++;
                }

                if (indicatorValues["RelativeVolume"][i].HasValue)
                {
                    sentiment += (indicatorValues["RelativeVolume"][i] ?? 0) / 10;
                    componentsCount++;
                }

                if (componentsCount > 0)
                {
                    sentiment /= componentsCount;
                    string sentimentCategory = CategorizeSentiment(sentiment);
                    sentiments.Add((candles[i].Date.Ticks, sentiment, sentimentCategory));
                }
            }

            UpdateSentiments(connection, transaction, tableName, productId, granularity, sentiments);
        }

        private static Dictionary<string, List<decimal?>> GetIndicatorValues(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, string[] indicators)
        {
            var result = indicators.ToDictionary(i => i, _ => new List<decimal?>());
            string query = $"SELECT StartDate, {string.Join(", ", indicators)} FROM {tableName} WHERE ProductId = @ProductId AND Granularity = @Granularity ORDER BY StartDate";

            using var command = new SqliteCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@ProductId", productId);
            command.Parameters.AddWithValue("@Granularity", granularity);

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                for (int i = 0; i < indicators.Length; i++)
                {
                    result[indicators[i]].Add(reader.IsDBNull(i + 1) ? (decimal?)null : reader.GetDecimal(i + 1));
                }
            }

            return result;
        }

        private static string CategorizeSentiment(decimal sentiment)
        {
            if (sentiment > 0.6m) return "Very Bullish";
            if (sentiment > 0.2m) return "Bullish";
            if (sentiment < -0.6m) return "Very Bearish";
            if (sentiment < -0.2m) return "Bearish";
            return "Neutral";
        }

        private static void UpdateSentiments(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal Sentiment, string Category)> sentiments)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET CompositeSentiment = @CompositeSentiment,
                    SentimentCategory = @SentimentCategory
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@CompositeSentiment", SqliteType.Real);
            command.Parameters.Add("@SentimentCategory", SqliteType.Text);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in sentiments.Chunk(BatchSize))
            {
                foreach (var (dateTicks, sentiment, category) in batch)
                {
                    command.Parameters["@CompositeSentiment"].Value = (double)sentiment;
                    command.Parameters["@SentimentCategory"].Value = category;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}