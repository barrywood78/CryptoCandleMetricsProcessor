using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class LaggedFeaturesIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var rsiResults = candles.GetRsi(14).ToList();
            var emaResults = candles.GetEma(14).ToList();
            var atrResults = candles.GetAtr(14).ToList();
            var macdResults = candles.GetMacd().ToList();
            var bbResults = candles.GetBollingerBands(20, 2).ToList();
            var stochResults = candles.GetStoch(14, 3, 3).ToList();

            var results = new List<LaggedFeatures>();

            for (int i = 3; i < candles.Count; i++)
            {
                if (i - 4 < 0) continue;

                results.Add(new LaggedFeatures
                {
                    DateTicks = candles[i].Date.Ticks,
                    LaggedClose1 = candles[i - 1].Close,
                    LaggedClose2 = candles[i - 2].Close,
                    LaggedClose3 = candles[i - 3].Close,
                    LaggedRSI1 = rsiResults[i - 1]?.Rsi,
                    LaggedRSI2 = rsiResults[i - 2]?.Rsi,
                    LaggedRSI3 = rsiResults[i - 3]?.Rsi,
                    LaggedReturn1 = candles[i - 1].Close / candles[i - 2].Close - 1,
                    LaggedReturn2 = candles[i - 2].Close / candles[i - 3].Close - 1,
                    LaggedReturn3 = candles[i - 3].Close / candles[i - 4].Close - 1,
                    LaggedEMA1 = (decimal?)(emaResults[i - 1]?.Ema),
                    LaggedEMA2 = (decimal?)(emaResults[i - 2]?.Ema),
                    LaggedEMA3 = (decimal?)(emaResults[i - 3]?.Ema),
                    LaggedATR1 = atrResults[i - 1]?.Atr,
                    LaggedATR2 = atrResults[i - 2]?.Atr,
                    LaggedATR3 = atrResults[i - 3]?.Atr,
                    LaggedMACD1 = macdResults[i - 1]?.Macd,
                    LaggedMACD2 = macdResults[i - 2]?.Macd,
                    LaggedMACD3 = macdResults[i - 3]?.Macd,
                    LaggedBollingerUpper1 = bbResults[i - 1]?.UpperBand,
                    LaggedBollingerUpper2 = bbResults[i - 2]?.UpperBand,
                    LaggedBollingerUpper3 = bbResults[i - 3]?.UpperBand,
                    LaggedBollingerLower1 = bbResults[i - 1]?.LowerBand,
                    LaggedBollingerLower2 = bbResults[i - 2]?.LowerBand,
                    LaggedBollingerLower3 = bbResults[i - 3]?.LowerBand,
                    LaggedBollingerPercentB1 = bbResults[i - 1]?.PercentB,
                    LaggedBollingerPercentB2 = bbResults[i - 2]?.PercentB,
                    LaggedBollingerPercentB3 = bbResults[i - 3]?.PercentB,
                    LaggedStochK1 = stochResults[i - 1]?.K,
                    LaggedStochK2 = stochResults[i - 2]?.K,
                    LaggedStochK3 = stochResults[i - 3]?.K,
                    LaggedStochD1 = stochResults[i - 1]?.D,
                    LaggedStochD2 = stochResults[i - 2]?.D,
                    LaggedStochD3 = stochResults[i - 3]?.D
                });
            }

            UpdateLaggedFeatures(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateLaggedFeatures(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<LaggedFeatures> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET Lagged_Close_1 = @LaggedClose1, Lagged_Close_2 = @LaggedClose2, Lagged_Close_3 = @LaggedClose3,
                    Lagged_RSI_1 = @LaggedRSI1, Lagged_RSI_2 = @LaggedRSI2, Lagged_RSI_3 = @LaggedRSI3,
                    Lagged_Return_1 = @LaggedReturn1, Lagged_Return_2 = @LaggedReturn2, Lagged_Return_3 = @LaggedReturn3,
                    Lagged_EMA_1 = @LaggedEMA1, Lagged_EMA_2 = @LaggedEMA2, Lagged_EMA_3 = @LaggedEMA3,
                    Lagged_ATR_1 = @LaggedATR1, Lagged_ATR_2 = @LaggedATR2, Lagged_ATR_3 = @LaggedATR3,
                    Lagged_MACD_1 = @LaggedMACD1, Lagged_MACD_2 = @LaggedMACD2, Lagged_MACD_3 = @LaggedMACD3,
                    Lagged_BollingerUpper_1 = @LaggedBollingerUpper1, Lagged_BollingerUpper_2 = @LaggedBollingerUpper2, Lagged_BollingerUpper_3 = @LaggedBollingerUpper3,
                    Lagged_BollingerLower_1 = @LaggedBollingerLower1, Lagged_BollingerLower_2 = @LaggedBollingerLower2, Lagged_BollingerLower_3 = @LaggedBollingerLower3,
                    Lagged_BollingerPercentB_1 = @LaggedBollingerPercentB1, Lagged_BollingerPercentB_2 = @LaggedBollingerPercentB2, Lagged_BollingerPercentB_3 = @LaggedBollingerPercentB3,
                    Lagged_StochK_1 = @LaggedStochK1, Lagged_StochK_2 = @LaggedStochK2, Lagged_StochK_3 = @LaggedStochK3,
                    Lagged_StochD_1 = @LaggedStochD1, Lagged_StochD_2 = @LaggedStochD2, Lagged_StochD_3 = @LaggedStochD3
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);
            AddParametersForLaggedFeatures(command);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var result in batch)
                {
                    SetParameterValues(command, result);
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void AddParametersForLaggedFeatures(SqliteCommand command)
        {
            var paramNames = typeof(LaggedFeatures).GetProperties()
                .Where(p => p.Name != "DateTicks")
                .Select(p => "@" + p.Name);

            foreach (var paramName in paramNames)
            {
                command.Parameters.Add(paramName, SqliteType.Real);
            }
        }

        private static void SetParameterValues(SqliteCommand command, LaggedFeatures result)
        {
            command.Parameters["@StartDate"].Value = result.DateTicks;

            foreach (var prop in typeof(LaggedFeatures).GetProperties().Where(p => p.Name != "DateTicks"))
            {
                var value = prop.GetValue(result);
                command.Parameters["@" + prop.Name].Value = value ?? DBNull.Value;
            }
        }

        private class LaggedFeatures
        {
            public long DateTicks { get; set; }
            public decimal LaggedClose1 { get; set; }
            public decimal LaggedClose2 { get; set; }
            public decimal LaggedClose3 { get; set; }
            public double? LaggedRSI1 { get; set; }
            public double? LaggedRSI2 { get; set; }
            public double? LaggedRSI3 { get; set; }
            public decimal LaggedReturn1 { get; set; }
            public decimal LaggedReturn2 { get; set; }
            public decimal LaggedReturn3 { get; set; }
            public decimal? LaggedEMA1 { get; set; }
            public decimal? LaggedEMA2 { get; set; }
            public decimal? LaggedEMA3 { get; set; }
            public double? LaggedATR1 { get; set; }
            public double? LaggedATR2 { get; set; }
            public double? LaggedATR3 { get; set; }
            public double? LaggedMACD1 { get; set; }
            public double? LaggedMACD2 { get; set; }
            public double? LaggedMACD3 { get; set; }
            public double? LaggedBollingerUpper1 { get; set; }
            public double? LaggedBollingerUpper2 { get; set; }
            public double? LaggedBollingerUpper3 { get; set; }
            public double? LaggedBollingerLower1 { get; set; }
            public double? LaggedBollingerLower2 { get; set; }
            public double? LaggedBollingerLower3 { get; set; }
            public double? LaggedBollingerPercentB1 { get; set; }
            public double? LaggedBollingerPercentB2 { get; set; }
            public double? LaggedBollingerPercentB3 { get; set; }
            public double? LaggedStochK1 { get; set; }
            public double? LaggedStochK2 { get; set; }
            public double? LaggedStochK3 { get; set; }
            public double? LaggedStochD1 { get; set; }
            public double? LaggedStochD2 { get; set; }
            public double? LaggedStochD3 { get; set; }
        }
    }
}