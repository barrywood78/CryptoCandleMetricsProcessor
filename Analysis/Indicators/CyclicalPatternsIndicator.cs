using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System.Numerics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CyclicalPatternsIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            int cyclePeriod = GetCyclePeriodForGranularity(granularity);
            var results = new ConcurrentDictionary<DateTime, (int? DominantPeriod, double? Phase, bool? IsBullishPhase)>();

            // Pre-compute closing prices array
            double[] closingPrices = candles.Select(c => (double)c.Close).ToArray();

            // Parallel processing of candles with chunking
            int chunkSize = Math.Max(1000, cyclePeriod);
            Parallel.ForEach(Partitioner.Create(0, candles.Count, chunkSize), range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    if (i >= cyclePeriod)
                    {
                        var cycleData = new Span<double>(closingPrices, i - cyclePeriod, cyclePeriod);
                        int? dominantPeriod = FindSimpleDominantPeriod(cycleData);

                        double? phase = null;
                        bool? isBullishPhase = null;

                        if (dominantPeriod.HasValue && dominantPeriod.Value != 0)
                        {
                            phase = CalculatePhase(i % dominantPeriod.Value, dominantPeriod.Value);
                            isBullishPhase = cycleData[^1] > cycleData.ToArray().Average();
                        }

                        results[candles[i].Date] = (dominantPeriod, phase, isBullishPhase);
                    }
                    else
                    {
                        results[candles[i].Date] = (null, null, null);
                    }
                }
            });

            // Batch update
            const int batchSize = 5000;
            string updateQuery = $@"
                UPDATE {tableName}
                SET CycleDominantPeriod = @DominantPeriod,
                    CyclePhase = @Phase,
                    IsBullishCyclePhase = @IsBullishPhase
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@DominantPeriod", SqliteType.Integer);
            command.Parameters.Add("@Phase", SqliteType.Real);
            command.Parameters.Add("@IsBullishPhase", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Text);

            foreach (var batch in results.Keys.Chunk(batchSize))
            {
                foreach (var date in batch)
                {
                    var (dominantPeriod, phase, isBullishPhase) = results[date];
                    command.Parameters["@DominantPeriod"].Value = dominantPeriod.HasValue ? (object)dominantPeriod.Value : DBNull.Value;
                    command.Parameters["@Phase"].Value = phase.HasValue ? (object)phase.Value : DBNull.Value;
                    command.Parameters["@IsBullishPhase"].Value = isBullishPhase.HasValue ? (object)(isBullishPhase.Value ? 1 : 0) : DBNull.Value;
                    command.Parameters["@StartDate"].Value = date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    command.ExecuteNonQuery();
                }
            }
        }

        private static int GetCyclePeriodForGranularity(string granularity) => granularity switch
        {
            "ONE_MINUTE" => 60 * 24 * 7,
            "FIVE_MINUTE" => 12 * 24 * 7,
            "FIFTEEN_MINUTE" => 4 * 24 * 7,
            "ONE_HOUR" => 24 * 7,
            "ONE_DAY" => 365,
            _ => throw new ArgumentException($"Unsupported granularity: {granularity}")
        };

        private static int? FindSimpleDominantPeriod(Span<double> data)
        {
            var autocorrelation = FastAutocorrelation(data);
            for (int i = 1; i < autocorrelation.Length - 1; i++)
            {
                if (autocorrelation[i] > autocorrelation[i - 1] && autocorrelation[i] > autocorrelation[i + 1])
                {
                    return i;
                }
            }
            return data.Length / 2; // fallback
        }

        private static double[] FastAutocorrelation(Span<double> data)
        {
            int n = data.Length;
            var fft = new Complex[n];
            for (int i = 0; i < n; i++)
            {
                fft[i] = new Complex(data[i], 0);
            }

            FourierTransform.FFT(fft, FourierTransform.Direction.Forward);

            for (int i = 0; i < n; i++)
            {
                fft[i] = Complex.Multiply(fft[i], Complex.Conjugate(fft[i]));
            }

            FourierTransform.FFT(fft, FourierTransform.Direction.Backward);

            var result = new double[n];
            double scale = 1.0 / (n * data[0]);
            for (int i = 0; i < n; i++)
            {
                result[i] = fft[i].Real * scale;
            }

            return result;
        }

        private static double CalculatePhase(int position, int period) => 2 * Math.PI * position / period;
    }

    // Simple FFT implementation
    public static class FourierTransform
    {
        public enum Direction { Forward = 1, Backward = -1 };

        public static void FFT(Complex[] data, Direction direction)
        {
            int n = data.Length;
            if (n <= 1) return;

            Complex[] even = new Complex[n / 2];
            Complex[] odd = new Complex[n / 2];

            for (int i = 0; i < n / 2; i++)
            {
                even[i] = data[2 * i];
                odd[i] = data[2 * i + 1];
            }

            FFT(even, direction);
            FFT(odd, direction);

            double angle = 2 * Math.PI / n * (int)direction;
            Complex w = Complex.One;
            Complex wn = new Complex(Math.Cos(angle), Math.Sin(angle));

            for (int k = 0; k < n / 2; k++)
            {
                data[k] = even[k] + w * odd[k];
                data[k + n / 2] = even[k] - w * odd[k];
                if (direction == Direction.Backward)
                {
                    data[k] /= 2;
                    data[k + n / 2] /= 2;
                }
                w *= wn;
            }
        }
    }
}