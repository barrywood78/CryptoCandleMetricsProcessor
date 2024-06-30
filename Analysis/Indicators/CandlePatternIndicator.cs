using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using Microsoft.Data.Sqlite;
using TicTacTec.TA.Library;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CandlePatternIndicator
    {
        // Custom delegate to match TA-Lib function signatures
        private delegate Core.RetCode CandlePatternFunction(int startIdx, int endIdx, float[] inOpen, float[] inHigh, float[] inLow, float[] inClose, out int outBegIdx, out int outNbElement, int[] outInteger);

        private static readonly Dictionary<string, int> BuyPatternRankings = new Dictionary<string, int>
        {
            {"CDL3LINESTRIKE", 1},
            {"CDLINVERTEDHAMMER", 6},
            {"CDLMATCHINGLOW", 7},
            {"CDLABANDONEDBABY", 8},
            {"CDLBREAKAWAY", 10},
            {"CDLMORNINGSTAR", 12},
            {"CDLPIERCING", 13},
            {"CDLSTICKSANDWHICH", 14},
            {"CDLTHRUSTING", 15},
            {"CDLINNECK", 17},
            {"CDL3INSIDE", 20},
            {"CDLHOMINGPIGEON", 21},
            {"CDLMORNINGDOJISTAR", 25},
            {"CDLBELTHOLD", 62},
            {"CDLHAMMER", 65},
            {"CDLENGULFING", 84},
        };

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            int[] outInteger = new int[candles.Count];
            int outBegIdx, outNbElement;

            float[] open = candles.Select(c => (float)c.Open).ToArray();
            float[] high = candles.Select(c => (float)c.High).ToArray();
            float[] low = candles.Select(c => (float)c.Low).ToArray();
            float[] close = candles.Select(c => (float)c.Close).ToArray();

            for (int i = 0; i < candles.Count; i++)
            {
                string bestPattern = "NO_PATTERN";
                int bestRank = int.MaxValue;
                int matchCount = 0;

                foreach (var pattern in BuyPatternRankings.Keys)
                {
                    CandlePatternFunction patternFunction = GetPatternRecognition(pattern);
                    Core.RetCode retCode = patternFunction(0, candles.Count - 1, open, high, low, close, out outBegIdx, out outNbElement, outInteger);

                    if (retCode == Core.RetCode.Success && i >= outBegIdx && i < outBegIdx + outNbElement && outInteger[i - outBegIdx] > 0)
                    {
                        matchCount++;
                        if (BuyPatternRankings[pattern] < bestRank)
                        {
                            bestPattern = pattern;
                            bestRank = BuyPatternRankings[pattern];
                        }
                    }
                }

                string updateQuery = $@"
                UPDATE {tableName}
                SET CandlePattern = @CandlePattern,
                    CandlePatternRank = @PatternRank,
                    CandlePatternMatchCount = @MatchCount
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@CandlePattern", bestPattern);
                    command.Parameters.AddWithValue("@PatternRank", bestPattern == "NO_PATTERN" ? DBNull.Value : bestRank);
                    command.Parameters.AddWithValue("@MatchCount", matchCount);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static CandlePatternFunction GetPatternRecognition(string pattern)
        {
            switch (pattern)
            {
                case "CDL3LINESTRIKE": return Core.Cdl3LineStrike;
                case "CDLINVERTEDHAMMER": return Core.CdlInvertedHammer;
                case "CDLMATCHINGLOW": return Core.CdlMatchingLow;
                case "CDLABANDONEDBABY":
                    return (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                        => Core.CdlAbandonedBaby(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger);
                case "CDLBREAKAWAY": return Core.CdlBreakaway;
                case "CDLMORNINGSTAR":
                    return (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                        => Core.CdlMorningStar(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger);
                case "CDLPIERCING": return Core.CdlPiercing;
                case "CDLSTICKSANDWHICH": return Core.CdlStickSandwhich;
                case "CDLTHRUSTING": return Core.CdlThrusting;
                case "CDLINNECK": return Core.CdlInNeck;
                case "CDL3INSIDE": return Core.Cdl3Inside;
                case "CDLHOMINGPIGEON": return Core.CdlHomingPigeon;
                case "CDLMORNINGDOJISTAR":
                    return (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                        => Core.CdlMorningDojiStar(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger);
                case "CDLBELTHOLD": return Core.CdlBeltHold;
                case "CDLHAMMER": return Core.CdlHammer;
                case "CDLENGULFING": return Core.CdlEngulfing;
                default: throw new ArgumentException($"Pattern {pattern} not supported.");
            }
        }
    }
}
