using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CyclicalPatternsIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            //Console.WriteLine($"Starting CyclicalPatternsIndicator calculation for {productId} - {granularity}");
            int cyclePeriod = GetCyclePeriodForGranularity(granularity);
            //Console.WriteLine($"Cycle period for {granularity}: {cyclePeriod}");
            //Console.WriteLine($"Total candles: {candles.Count}");

            try
            {
                int updatedRows = 0;
                for (int i = 0; i < candles.Count; i++)
                {
                    //Console.WriteLine($"Processing candle {i + 1} of {candles.Count}");
                    int? dominantPeriod = null;
                    double? phase = null;
                    bool? isBullishPhase = null;

                    if (i >= cyclePeriod)
                    {
                        var cycleData = candles.Skip(i - cyclePeriod).Take(cyclePeriod).Select(c => (double)c.Close).ToArray();
                        //Console.WriteLine($"Cycle data points: {cycleData.Length}");
                        //Console.WriteLine($"First price in cycle: {cycleData.First()}, Last price: {cycleData.Last()}");

                        // Simplified dominant period calculation
                        dominantPeriod = FindSimpleDominantPeriod(cycleData);
                        //Console.WriteLine($"Calculated Dominant period: {dominantPeriod}");

                        if (dominantPeriod.HasValue && dominantPeriod.Value != 0)
                        {
                            phase = CalculatePhase(i % dominantPeriod.Value, dominantPeriod.Value);
                            //Console.WriteLine($"Calculated Phase: {phase}");

                            isBullishPhase = cycleData.Last() > cycleData.Average();
                            //Console.WriteLine($"Is Bullish Phase: {isBullishPhase}");
                        }
                        else
                        {
                            //Console.WriteLine("Warning: Invalid dominant period calculated. Setting values to null.");
                        }
                    }
                    else
                    {
                        //Console.WriteLine($"Skipping full calculation for candle {i}, not enough data yet");
                    }

                    string updateQuery = $@"
                    UPDATE {tableName}
                    SET CycleDominantPeriod = @DominantPeriod,
                        CyclePhase = @Phase,
                        IsBullishCyclePhase = @IsBullishPhase
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        if (dominantPeriod.HasValue)
                            command.Parameters.AddWithValue("@DominantPeriod", dominantPeriod.Value);
                        else
                            command.Parameters.AddWithValue("@DominantPeriod", DBNull.Value);

                        if (phase.HasValue)
                            command.Parameters.AddWithValue("@Phase", phase.Value);
                        else
                            command.Parameters.AddWithValue("@Phase", DBNull.Value);

                        if (isBullishPhase.HasValue)
                            command.Parameters.AddWithValue("@IsBullishPhase", isBullishPhase.Value);
                        else
                            command.Parameters.AddWithValue("@IsBullishPhase", DBNull.Value);

                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                        int rowsAffected = command.ExecuteNonQuery();
                        updatedRows += rowsAffected;
                        //Console.WriteLine($"Rows affected by update: {rowsAffected}");

                        if (rowsAffected == 0)
                        {
                            //Console.WriteLine("Warning: No rows were updated. Verify WHERE clause conditions.");
                            //Console.WriteLine($"ProductId: {productId}, Granularity: {granularity}, StartDate: {candles[i].Date}");
                        }
                    }

                    if (i % 1000 == 0 && i > 0)
                    {
                        //Console.WriteLine($"Processed {i} candles. Total updates so far: {updatedRows}");
                    }
                }

                //Console.WriteLine($"CyclicalPatternsIndicator calculation completed. Total candles processed: {candles.Count}");
                //Console.WriteLine($"Total rows updated in database: {updatedRows}");
            }
            catch (Exception)
            {
                //Console.WriteLine($"Error in CyclicalPatternsIndicator: {ex.Message}");
                //Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                throw;
            }
        }

        private static int GetCyclePeriodForGranularity(string granularity)
        {
            return granularity switch
            {
                "ONE_MINUTE" => 60 * 24 * 7,  // One week
                "FIVE_MINUTE" => 12 * 24 * 7,  // One week
                "FIFTEEN_MINUTE" => 4 * 24 * 7,  // One week
                "ONE_HOUR" => 24 * 7,  // One week
                "ONE_DAY" => 365,  // One year
                _ => throw new ArgumentException($"Unsupported granularity: {granularity}")
            };
        }

        private static int FindSimpleDominantPeriod(double[] data)
        {
            //Console.WriteLine("Starting FindSimpleDominantPeriod calculation");
            var autocorrelation = Autocorrelation(data);
            for (int i = 1; i < autocorrelation.Length - 1; i++)
            {
                if (autocorrelation[i] > autocorrelation[i - 1] && autocorrelation[i] > autocorrelation[i + 1])
                {
                    //Console.WriteLine($"Dominant period found: {i}");
                    return i;
                }
            }
            //Console.WriteLine("No clear dominant period found, using fallback");
            return data.Length / 2; // fallback
        }

        private static double[] Autocorrelation(double[] data)
        {
            //Console.WriteLine("Calculating Autocorrelation");
            int n = data.Length;
            double mean = data.Average();
            double[] centered = data.Select(x => x - mean).ToArray();
            double[] result = new double[n];

            for (int lag = 0; lag < n; lag++)
            {
                double numerator = 0, denominator = 0;
                for (int i = 0; i < n - lag; i++)
                {
                    numerator += centered[i] * centered[i + lag];
                    denominator += centered[i] * centered[i];
                }
                result[lag] = numerator / denominator;
            }

            //Console.WriteLine("Autocorrelation calculation completed");
            return result;
        }

        private static double CalculatePhase(int position, int period)
        {
            return 2 * Math.PI * position / period;
        }
    }
}