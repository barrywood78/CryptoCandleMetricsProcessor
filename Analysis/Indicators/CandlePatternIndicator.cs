using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using TicTacTec.TA.Library;
using Skender.Stock.Indicators;
using SwiftLogger.Enums;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CandlePatternIndicator
    {
        private delegate Core.RetCode CandlePatternFunction(int startIdx, int endIdx, float[] inOpen, float[] inHigh, float[] inLow, float[] inClose, out int outBegIdx, out int outNbElement, int[] outInteger);

        private static readonly Dictionary<string, int> BuyPatternRankings = new Dictionary<string, int>
        {
            {"CDL3LINESTRIKE", 1},
            {"CDLMORNINGSTAR", 2},
            {"CDLENGULFING", 3},
            {"CDLABANDONEDBABY", 4},
            {"CDLMORNINGDOJISTAR", 5},
            {"CDLHAMMER", 6},
            {"CDLBREAKAWAY", 7},
            {"CDLPIERCING", 8},
            {"CDLBELTHOLD", 9},
            {"CDLMATCHINGLOW", 10},
            {"CDLHOMINGPIGEON", 11},
            {"CDLINVERTEDHAMMER", 12},
            {"CDL3INSIDE", 13},
            {"CDLTHRUSTING", 14},
            {"CDLSTICKSANDWHICH", 15},
            {"CDLINNECK", 16}
        };

        private static readonly Dictionary<string, CandlePatternFunction> PatternFunctions = new Dictionary<string, CandlePatternFunction>
        {
            {"CDL3LINESTRIKE", Core.Cdl3LineStrike},
            {"CDLINVERTEDHAMMER", Core.CdlInvertedHammer},
            {"CDLMATCHINGLOW", Core.CdlMatchingLow},
            {"CDLABANDONEDBABY", (int startIdx, int endIdx, float[] inOpen, float[] inHigh, float[] inLow, float[] inClose, out int outBegIdx, out int outNbElement, int[] outInteger)
                => Core.CdlAbandonedBaby(startIdx, endIdx, inOpen, inHigh, inLow, inClose, 0.3f, out outBegIdx, out outNbElement, outInteger)},
            {"CDLBREAKAWAY", Core.CdlBreakaway},
            {"CDLMORNINGSTAR", (int startIdx, int endIdx, float[] inOpen, float[] inHigh, float[] inLow, float[] inClose, out int outBegIdx, out int outNbElement, int[] outInteger)
                => Core.CdlMorningStar(startIdx, endIdx, inOpen, inHigh, inLow, inClose, 0.3f, out outBegIdx, out outNbElement, outInteger)},
            {"CDLPIERCING", Core.CdlPiercing},
            {"CDLSTICKSANDWHICH", Core.CdlStickSandwhich},
            {"CDLTHRUSTING", Core.CdlThrusting},
            {"CDLINNECK", Core.CdlInNeck},
            {"CDL3INSIDE", Core.Cdl3Inside},
            {"CDLHOMINGPIGEON", Core.CdlHomingPigeon},
            {"CDLMORNINGDOJISTAR", (int startIdx, int endIdx, float[] inOpen, float[] inHigh, float[] inLow, float[] inClose, out int outBegIdx, out int outNbElement, int[] outInteger)
                => Core.CdlMorningDojiStar(startIdx, endIdx, inOpen, inHigh, inLow, inClose, 0.3f, out outBegIdx, out outNbElement, outInteger)},
            {"CDLBELTHOLD", Core.CdlBeltHold},
            {"CDLHAMMER", Core.CdlHammer},
            {"CDLENGULFING", Core.CdlEngulfing},
        };

        private static int GetScaledRanking(int rank)
        {
            if (rank <= 3) return 10;  // Top 3 patterns get the highest rank
            if (rank <= 7) return 8;   // Next 4 patterns
            if (rank <= 10) return 6;  // Next 3 patterns
            if (rank <= 13) return 4;  // Next 3 patterns
            if (rank <= 15) return 2;  // Next 2 patterns
            return 1;                  // Lowest ranked pattern
        }

        public static async Task CalculateAsync(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, SwiftLogger.SwiftLogger logger)
        {
            const int batchSize = 5000;
            var patternResults = new Dictionary<string, int[]>();

            float[] open = candles.Select(c => (float)c.Open).ToArray();
            float[] high = candles.Select(c => (float)c.High).ToArray();
            float[] low = candles.Select(c => (float)c.Low).ToArray();
            float[] close = candles.Select(c => (float)c.Close).ToArray();

            // Calculate all patterns in parallel
            Parallel.ForEach(PatternFunctions, patternFunc =>
            {
                var result = new int[candles.Count];
                patternFunc.Value(0, candles.Count - 1, open, high, low, close, out _, out _, result);
                lock (patternResults)
                {
                    patternResults[patternFunc.Key] = result;
                }
            });

            // Prepare the SQL command
            using var command = new SqliteCommand(@"
                UPDATE " + tableName + @"
                SET CandlePattern = @CandlePattern,
                    CandlePatternRank = @CandlePatternRank,
                    CandlePatternScaledRank = @CandlePatternScaledRank,
                    CandlePatternMatchCount = @CandlePatternMatchCount
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate;", connection, transaction);

            // Add parameters
            command.Parameters.Add("@CandlePattern", SqliteType.Text);
            command.Parameters.Add("@CandlePatternRank", SqliteType.Integer);
            command.Parameters.Add("@CandlePatternScaledRank", SqliteType.Integer);
            command.Parameters.Add("@CandlePatternMatchCount", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Text);

            for (int i = 0; i < candles.Count; i += batchSize)
            {
                int endIdx = Math.Min(i + batchSize, candles.Count);
                for (int j = i; j < endIdx; j++)
                {
                    string bestPattern = "NO_PATTERN";
                    int bestRank = int.MaxValue;
                    int matchCount = 0;

                    foreach (var pattern in BuyPatternRankings.Keys)
                    {
                        if (patternResults[pattern][j] > 0)
                        {
                            matchCount++;
                            if (BuyPatternRankings[pattern] < bestRank)
                            {
                                bestPattern = pattern;
                                bestRank = BuyPatternRankings[pattern];
                            }
                        }
                    }

                    command.Parameters["@CandlePattern"].Value = bestPattern;
                    command.Parameters["@CandlePatternRank"].Value = bestPattern == "NO_PATTERN" ? DBNull.Value : (object)bestRank;
                    command.Parameters["@CandlePatternScaledRank"].Value = bestPattern == "NO_PATTERN" ? DBNull.Value : (object)GetScaledRanking(bestRank);
                    command.Parameters["@CandlePatternMatchCount"].Value = matchCount;
                    command.Parameters["@StartDate"].Value = candles[j].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    command.ExecuteNonQuery();
                }

                // Commit the transaction after each batch
                transaction.Commit();
                // Start a new transaction for the next batch
                transaction = connection.BeginTransaction();
                command.Transaction = transaction;

                await logger.Log(LogLevel.Information, $"{productId} - {granularity}: CandlePatternIndicator - Processed {endIdx}/{candles.Count} rows.");
            }

            // Commit any remaining changes
            transaction.Commit();

            // Memory management
            patternResults.Clear();
            GC.Collect();
        }
    }
}