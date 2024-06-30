using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CompositeMarketSentimentIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var rsiResults = GetIndicatorValues(connection, transaction, tableName, productId, granularity, "RSI");
            var macdHistogram = GetIndicatorValues(connection, transaction, tableName, productId, granularity, "MACD_Histogram");
            var bollingerPercentB = GetIndicatorValues(connection, transaction, tableName, productId, granularity, "BB_PercentB");
            var adx = GetIndicatorValues(connection, transaction, tableName, productId, granularity, "ADX");
            var relativeVolume = GetIndicatorValues(connection, transaction, tableName, productId, granularity, "RelativeVolume");

            for (int i = 0; i < candles.Count; i++)
            {
                decimal sentiment = 0;
                int componentsCount = 0;

                decimal? rsiValue = rsiResults[i];
                if (rsiValue.HasValue)
                {
                    sentiment += ((rsiValue ?? 0) - 50) / 50;
                    componentsCount++;
                }

                decimal? macdValue = macdHistogram[i];
                if (macdValue.HasValue)
                {
                    sentiment += (macdValue ?? 0) / candles[i].Close;
                    componentsCount++;
                }

                decimal? bbValue = bollingerPercentB[i];
                if (bbValue.HasValue)
                {
                    sentiment += (bbValue ?? 0) - 0.5m;
                    componentsCount++;
                }

                decimal? adxValue = adx[i];
                if (adxValue.HasValue)
                {
                    sentiment += ((adxValue ?? 0) / 100) - 0.5m;
                    componentsCount++;
                }

                decimal? rvValue = relativeVolume[i];
                if (rvValue.HasValue)
                {
                    sentiment += (rvValue ?? 0) / 10;
                    componentsCount++;
                }

                if (componentsCount > 0)
                {
                    sentiment /= componentsCount;

                    string sentimentCategory;
                    if (sentiment > 0.6m) sentimentCategory = "Very Bullish";
                    else if (sentiment > 0.2m) sentimentCategory = "Bullish";
                    else if (sentiment < -0.6m) sentimentCategory = "Very Bearish";
                    else if (sentiment < -0.2m) sentimentCategory = "Bearish";
                    else sentimentCategory = "Neutral";

                    string updateQuery = $@"
                    UPDATE {tableName}
                    SET CompositeSentiment = @CompositeSentiment,
                        SentimentCategory = @SentimentCategory
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@CompositeSentiment", sentiment);
                        command.Parameters.AddWithValue("@SentimentCategory", sentimentCategory);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        private static List<decimal?> GetIndicatorValues(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, string indicatorName)
        {
            var values = new List<decimal?>();
            string query = $"SELECT {indicatorName} FROM {tableName} WHERE ProductId = @ProductId AND Granularity = @Granularity ORDER BY StartDate";

            using (var command = new SqliteCommand(query, connection, transaction))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        values.Add(reader.IsDBNull(0) ? (decimal?)null : reader.GetDecimal(0));
                    }
                }
            }

            return values;
        }
    }
}