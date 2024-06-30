using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
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

            var updateCommands = new StringBuilder();
            int batchSize = 1000;

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
                SET CandlePattern = '{bestPattern}',
                    CandlePatternRank = {(bestPattern == "NO_PATTERN" ? "NULL" : bestRank.ToString())},
                    CandlePatternMatchCount = {matchCount}
                WHERE ProductId = '{productId}'
                  AND Granularity = '{granularity}'
                  AND StartDate = '{candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}';";

                updateCommands.AppendLine(updateQuery);

                // Execute batch update every 1000 rows
                if ((i + 1) % batchSize == 0 || i == candles.Count - 1)
                {
                    using (var command = new SqliteCommand(updateCommands.ToString(), connection, transaction))
                    {
                        command.ExecuteNonQuery();
                    }
                    updateCommands.Clear();
                    Console.WriteLine($"CandlePatternIndicator - Processed {i + 1}/{candles.Count} rows.");
                }
            }
        }

        private static CandlePatternFunction GetPatternRecognition(string pattern)
        {
            return pattern switch
            {
                "CDL3LINESTRIKE" => Core.Cdl3LineStrike,
                "CDLINVERTEDHAMMER" => Core.CdlInvertedHammer,
                "CDLMATCHINGLOW" => Core.CdlMatchingLow,
                "CDLABANDONEDBABY" => (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                    => Core.CdlAbandonedBaby(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger),
                "CDLBREAKAWAY" => Core.CdlBreakaway,
                "CDLMORNINGSTAR" => (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                    => Core.CdlMorningStar(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger),
                "CDLPIERCING" => Core.CdlPiercing,
                "CDLSTICKSANDWHICH" => Core.CdlStickSandwhich,
                "CDLTHRUSTING" => Core.CdlThrusting,
                "CDLINNECK" => Core.CdlInNeck,
                "CDL3INSIDE" => Core.Cdl3Inside,
                "CDLHOMINGPIGEON" => Core.CdlHomingPigeon,
                "CDLMORNINGDOJISTAR" => (int startIdx, int endIdx, float[] open, float[] high, float[] low, float[] close, out int outBegIdx, out int outNbElement, int[] outInteger)
                    => Core.CdlMorningDojiStar(startIdx, endIdx, open, high, low, close, 0.3f, out outBegIdx, out outNbElement, outInteger),
                "CDLBELTHOLD" => Core.CdlBeltHold,
                "CDLHAMMER" => Core.CdlHammer,
                "CDLENGULFING" => Core.CdlEngulfing,
                _ => throw new ArgumentException($"Pattern {pattern} not supported."),
            };
        }
    }
}
