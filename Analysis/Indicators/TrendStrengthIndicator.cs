using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System.Globalization;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class TrendStrengthIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int adxPeriod = 14, int smaPeriod = 50)
        {
            var adxResults = candles.GetAdx(adxPeriod).ToList();
            var smaResults = candles.GetSma(smaPeriod).ToList();
            int trendDuration = 0;
            bool isUptrend = false;

            for (int i = smaPeriod - 1; i < candles.Count; i++)
            {
                if (adxResults[i].Adx.HasValue && smaResults[i].Sma.HasValue)
                {
                    decimal adxValue = (decimal)(adxResults[i].Adx ?? 0);
                    decimal priceToSma = candles[i].Close / (decimal)(smaResults[i].Sma ?? 1);
                    decimal trendStrength = adxValue * (priceToSma > 1 ? priceToSma : 1 / priceToSma);

                    // Determine if the trend has changed
                    bool currentIsUptrend = candles[i].Close > (decimal)(smaResults[i].Sma ?? 0);

                    if (currentIsUptrend != isUptrend)
                    {
                        trendDuration = 1;
                        isUptrend = currentIsUptrend;
                    }
                    else
                    {
                        trendDuration++;
                    }

                    string updateQuery = $@"
                    UPDATE {tableName}
                    SET TrendStrength = @TrendStrength,
                        TrendDuration = @TrendDuration,
                        IsUptrend = @IsUptrend
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@TrendStrength", trendStrength);
                        command.Parameters.AddWithValue("@TrendDuration", trendDuration);
                        command.Parameters.AddWithValue("@IsUptrend", isUptrend);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}