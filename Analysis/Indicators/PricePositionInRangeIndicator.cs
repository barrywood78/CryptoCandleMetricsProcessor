using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PricePositionInRangeIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            foreach (var candle in candles)
            {
                decimal pricePositionInRange;
                if (candle.High != candle.Low)
                {
                    pricePositionInRange = (candle.Close - candle.Low) / (candle.High - candle.Low);
                }
                else
                {
                    // Handle the case where High equals Low to avoid division by zero
                    pricePositionInRange = 0.5m; // Assuming mid-range when high equals low
                }

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET PricePositionInRange = @PricePositionInRange
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@PricePositionInRange", pricePositionInRange);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candle.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
