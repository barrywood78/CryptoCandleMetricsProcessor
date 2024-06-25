using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class LaggedFeaturesIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            // Calculate all indicators first
            var rsiResults = candles.GetRsi(14).ToList();
            var emaResults = candles.GetEma(14).ToList();
            var atrResults = candles.GetAtr(14).ToList();
            var macdResults = candles.GetMacd().ToList();
            var bbResults = candles.GetBollingerBands(20, 2).ToList();
            var stochResults = candles.GetStoch(14, 3, 3).ToList();

            for (int i = 3; i < candles.Count; i++)
            {
                if (i - 4 < 0) continue; // Ensure there are enough previous periods

                var laggedClose1 = candles[i - 1].Close;
                var laggedClose2 = candles[i - 2].Close;
                var laggedClose3 = candles[i - 3].Close;

                var laggedRSI1 = rsiResults[i - 1]?.Rsi;
                var laggedRSI2 = rsiResults[i - 2]?.Rsi;
                var laggedRSI3 = rsiResults[i - 3]?.Rsi;

                var laggedReturn1 = candles[i - 1].Close / candles[i - 2].Close - 1;
                var laggedReturn2 = candles[i - 2].Close / candles[i - 3].Close - 1;
                var laggedReturn3 = candles[i - 3].Close / candles[i - 4].Close - 1;

                var laggedEMA1 = emaResults[i - 1]?.Ema;
                var laggedEMA2 = emaResults[i - 2]?.Ema;
                var laggedEMA3 = emaResults[i - 3]?.Ema;

                var laggedATR1 = atrResults[i - 1]?.Atr;
                var laggedATR2 = atrResults[i - 2]?.Atr;
                var laggedATR3 = atrResults[i - 3]?.Atr;

                var laggedMACD1 = macdResults[i - 1]?.Macd;
                var laggedMACD2 = macdResults[i - 2]?.Macd;
                var laggedMACD3 = macdResults[i - 3]?.Macd;

                var laggedBollingerUpper1 = bbResults[i - 1]?.UpperBand;
                var laggedBollingerUpper2 = bbResults[i - 2]?.UpperBand;
                var laggedBollingerUpper3 = bbResults[i - 3]?.UpperBand;

                var laggedBollingerLower1 = bbResults[i - 1]?.LowerBand;
                var laggedBollingerLower2 = bbResults[i - 2]?.LowerBand;
                var laggedBollingerLower3 = bbResults[i - 3]?.LowerBand;

                var laggedBollingerPercentB1 = bbResults[i - 1]?.PercentB;
                var laggedBollingerPercentB2 = bbResults[i - 2]?.PercentB;
                var laggedBollingerPercentB3 = bbResults[i - 3]?.PercentB;

                var laggedStochK1 = stochResults[i - 1]?.K;
                var laggedStochK2 = stochResults[i - 2]?.K;
                var laggedStochK3 = stochResults[i - 3]?.K;

                var laggedStochD1 = stochResults[i - 1]?.D;
                var laggedStochD2 = stochResults[i - 2]?.D;
                var laggedStochD3 = stochResults[i - 3]?.D;

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET Lagged_Close_1 = @LaggedClose1,
                        Lagged_Close_2 = @LaggedClose2,
                        Lagged_Close_3 = @LaggedClose3,
                        Lagged_RSI_1 = @LaggedRSI1,
                        Lagged_RSI_2 = @LaggedRSI2,
                        Lagged_RSI_3 = @LaggedRSI3,
                        Lagged_Return_1 = @LaggedReturn1,
                        Lagged_Return_2 = @LaggedReturn2,
                        Lagged_Return_3 = @LaggedReturn3,
                        Lagged_EMA_1 = @LaggedEMA1,
                        Lagged_EMA_2 = @LaggedEMA2,
                        Lagged_EMA_3 = @LaggedEMA3,
                        Lagged_ATR_1 = @LaggedATR1,
                        Lagged_ATR_2 = @LaggedATR2,
                        Lagged_ATR_3 = @LaggedATR3,
                        Lagged_MACD_1 = @LaggedMACD1,
                        Lagged_MACD_2 = @LaggedMACD2,
                        Lagged_MACD_3 = @LaggedMACD3,
                        Lagged_BollingerUpper_1 = @LaggedBollingerUpper1,
                        Lagged_BollingerUpper_2 = @LaggedBollingerUpper2,
                        Lagged_BollingerUpper_3 = @LaggedBollingerUpper3,
                        Lagged_BollingerLower_1 = @LaggedBollingerLower1,
                        Lagged_BollingerLower_2 = @LaggedBollingerLower2,
                        Lagged_BollingerLower_3 = @LaggedBollingerLower3,
                        Lagged_BollingerPercentB_1 = @LaggedBollingerPercentB1,
                        Lagged_BollingerPercentB_2 = @LaggedBollingerPercentB2,
                        Lagged_BollingerPercentB_3 = @LaggedBollingerPercentB3,
                        Lagged_StochK_1 = @LaggedStochK1,
                        Lagged_StochK_2 = @LaggedStochK2,
                        Lagged_StochK_3 = @LaggedStochK3,
                        Lagged_StochD_1 = @LaggedStochD1,
                        Lagged_StochD_2 = @LaggedStochD2,
                        Lagged_StochD_3 = @LaggedStochD3
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@LaggedClose1", laggedClose1);
                    command.Parameters.AddWithValue("@LaggedClose2", laggedClose2);
                    command.Parameters.AddWithValue("@LaggedClose3", laggedClose3);
                    command.Parameters.AddWithValue("@LaggedRSI1", laggedRSI1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedRSI2", laggedRSI2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedRSI3", laggedRSI3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedReturn1", laggedReturn1);
                    command.Parameters.AddWithValue("@LaggedReturn2", laggedReturn2);
                    command.Parameters.AddWithValue("@LaggedReturn3", laggedReturn3);
                    command.Parameters.AddWithValue("@LaggedEMA1", laggedEMA1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedEMA2", laggedEMA2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedEMA3", laggedEMA3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedATR1", laggedATR1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedATR2", laggedATR2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedATR3", laggedATR3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedMACD1", laggedMACD1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedMACD2", laggedMACD2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedMACD3", laggedMACD3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerUpper1", laggedBollingerUpper1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerUpper2", laggedBollingerUpper2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerUpper3", laggedBollingerUpper3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerLower1", laggedBollingerLower1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerLower2", laggedBollingerLower2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerLower3", laggedBollingerLower3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerPercentB1", laggedBollingerPercentB1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerPercentB2", laggedBollingerPercentB2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedBollingerPercentB3", laggedBollingerPercentB3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochK1", laggedStochK1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochK2", laggedStochK2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochK3", laggedStochK3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochD1", laggedStochD1 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochD2", laggedStochD2 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@LaggedStochD3", laggedStochD3 ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
