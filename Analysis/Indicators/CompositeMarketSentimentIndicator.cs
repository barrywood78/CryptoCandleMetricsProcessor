using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CompositeMarketSentimentIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Assuming these indicators have already been calculated
            var rsiResults = GetIndicatorValues(connection, tableName, productId, granularity, "RSI");
            var macdHistogram = GetIndicatorValues(connection, tableName, productId, granularity, "MACD_Histogram");
            var bollingerPercentB = GetIndicatorValues(connection, tableName, productId, granularity, "BB_PercentB");
            var adx = GetIndicatorValues(connection, tableName, productId, granularity, "ADX");
            var relativeVolume = GetIndicatorValues(connection, tableName, productId, granularity, "RelativeVolume");

            for (int i = 0; i < candles.Count; i++)
            {
                decimal sentiment = 0;

                // RSI component (0 to 100 scale)
                sentiment += (rsiResults[i] - 50) / 50;

                // MACD Histogram component (normalized by closing price)
                sentiment += macdHistogram[i] / candles[i].Close;

                // Bollinger %B component (-0.5 to 0.5 scale)
                sentiment += bollingerPercentB[i] - 0.5m;

                // ADX component (0 to 1 scale, assuming ADX max is around 100)
                sentiment += (adx[i] / 100) - 0.5m;

                // Relative Volume component
                sentiment += relativeVolume[i] / 10; // Assuming most values fall between -5 and 5

                // Normalize the final sentiment score
                sentiment /= 5; // Divide by the number of components

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

        private static List<decimal> GetIndicatorValues(SqliteConnection connection, string tableName, string productId, string granularity, string indicatorName)
        {
            var values = new List<decimal>();
            string query = $"SELECT {indicatorName} FROM {tableName} WHERE ProductId = @ProductId AND Granularity = @Granularity ORDER BY StartDate";

            using (var command = new SqliteCommand(query, connection))
            {
                command.Parameters.AddWithValue("@ProductId", productId);
                command.Parameters.AddWithValue("@Granularity", granularity);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        values.Add(reader.GetDecimal(0));
                    }
                }
            }

            return values;
        }
    }
}
