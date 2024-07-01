using System.Collections.Generic;
using CryptoCandleMetricsProcessor.Models;

namespace CryptoCandleMetricsProcessor.Database
{
    public static class Fields
    {
        public static List<FieldDefinition> GetFields()
        {
            return new List<FieldDefinition>
            {
                new FieldDefinition { Name = "ProductId", DataType = "TEXT" },
                new FieldDefinition { Name = "Granularity", DataType = "TEXT" },
                new FieldDefinition { Name = "StartUnix", DataType = "INTEGER" },
                new FieldDefinition { Name = "StartDate", DataType = "TEXT" },
                new FieldDefinition { Name = "Low", DataType = "REAL" },
                new FieldDefinition { Name = "High", DataType = "REAL" },
                new FieldDefinition { Name = "Open", DataType = "REAL" },
                new FieldDefinition { Name = "Close", DataType = "REAL" },
                new FieldDefinition { Name = "Volume", DataType = "REAL" },
                new FieldDefinition { Name = "PriceUp", DataType = "INTEGER" }, // PriceUp (0 or 1)
                new FieldDefinition { Name = "PriceUpStreak", DataType = "INTEGER" },
                new FieldDefinition { Name = "BuyScore", DataType = "INTEGER" },
                new FieldDefinition { Name = "SMA", DataType = "REAL" },  // Simple Moving Average
                new FieldDefinition { Name = "EMA", DataType = "REAL" },  // Exponential Moving Average
                new FieldDefinition { Name = "ATR", DataType = "REAL" },  // Average True Range
                new FieldDefinition { Name = "RSI", DataType = "REAL" },  // Relative Strength Index
                new FieldDefinition { Name = "ADX", DataType = "REAL" },  // Average Directional Index
                new FieldDefinition { Name = "TEMA", DataType = "REAL" }, // Triple Exponential Moving Average
                new FieldDefinition { Name = "BB_SMA", DataType = "REAL" },  // Bollinger Bands SMA
                new FieldDefinition { Name = "BB_UpperBand", DataType = "REAL" },  // Bollinger Bands Upper Band
                new FieldDefinition { Name = "BB_LowerBand", DataType = "REAL" },  // Bollinger Bands Lower Band
                new FieldDefinition { Name = "BB_PercentB", DataType = "REAL" },  // Bollinger Bands Percent B
                new FieldDefinition { Name = "BB_ZScore", DataType = "REAL" },  // Bollinger Bands Z Score
                new FieldDefinition { Name = "BB_Width", DataType = "REAL" },  // Bollinger Bands Width
                new FieldDefinition { Name = "Stoch_K", DataType = "REAL" },  // Stochastic Oscillator %K
                new FieldDefinition { Name = "Stoch_D", DataType = "REAL" },  // Stochastic Oscillator %D
                new FieldDefinition { Name = "MACD", DataType = "REAL" },  // MACD Line
                new FieldDefinition { Name = "MACD_Signal", DataType = "REAL" },  // MACD Signal Line
                new FieldDefinition { Name = "MACD_Histogram", DataType = "REAL" },  // MACD Histogram
                new FieldDefinition { Name = "SuperTrend", DataType = "REAL" },  // SuperTrend
                new FieldDefinition { Name = "WilliamsR", DataType = "REAL" },  // Williams %R
                new FieldDefinition { Name = "OBV", DataType = "REAL" },  // On-Balance Volume (OBV)
                new FieldDefinition { Name = "RollingMean", DataType = "REAL" },
                new FieldDefinition { Name = "RollingStdDev", DataType = "REAL" },
                new FieldDefinition { Name = "RollingVariance", DataType = "REAL" },
                new FieldDefinition { Name = "RollingSkewness", DataType = "REAL" },
                new FieldDefinition { Name = "RollingKurtosis", DataType = "REAL" },
                new FieldDefinition { Name = "ADL", DataType = "REAL" },  // Accumulation/Distribution Line
                new FieldDefinition { Name = "CMF", DataType = "REAL" },  // Chaikin Money Flow
                new FieldDefinition { Name = "CCI", DataType = "REAL" },  // Commodity Channel Index
                new FieldDefinition { Name = "ParabolicSar", DataType = "REAL" },  // Parabolic SAR
                new FieldDefinition { Name = "PivotPoint", DataType = "REAL" },  // Pivot Point
                new FieldDefinition { Name = "Resistance1", DataType = "REAL" },  // Pivot Point - Resistance 1
                new FieldDefinition { Name = "Resistance2", DataType = "REAL" },  // Pivot Point - Resistance 2
                new FieldDefinition { Name = "Resistance3", DataType = "REAL" },  // Pivot Point - Resistance 3
                new FieldDefinition { Name = "Support1", DataType = "REAL" },  // Pivot Point - Support 1
                new FieldDefinition { Name = "Support2", DataType = "REAL" },  // Pivot Point - Support 2
                new FieldDefinition { Name = "Support3", DataType = "REAL" },  // Pivot Point - Support 3
                new FieldDefinition { Name = "FibRetracement_23_6", DataType = "REAL" },  // Fibonacci Retracement - 23.6%
                new FieldDefinition { Name = "FibRetracement_38_2", DataType = "REAL" },  // Fibonacci Retracement - 38.2%
                new FieldDefinition { Name = "FibRetracement_50", DataType = "REAL" },  // Fibonacci Retracement - 50%
                new FieldDefinition { Name = "FibRetracement_61_8", DataType = "REAL" },  // Fibonacci Retracement - 61.8%
                new FieldDefinition { Name = "FibRetracement_78_6", DataType = "REAL" },  // Fibonacci Retracement - 76.4%
                new FieldDefinition { Name = "Lagged_Close_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Close_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Close_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_3", DataType = "REAL" },
                new FieldDefinition { Name = "PriceChangePercent", DataType = "REAL" },
                new FieldDefinition { Name = "PricePositionInRange", DataType = "REAL" },
                new FieldDefinition { Name = "VolumeChangePercent", DataType = "REAL" },
                new FieldDefinition { Name = "RelativeVolume", DataType = "REAL" },
                new FieldDefinition { Name = "VolumeProfile", DataType = "TEXT" },
                new FieldDefinition { Name = "ATRPercent", DataType = "REAL" },
                new FieldDefinition { Name = "RSIChange", DataType = "REAL" },
                new FieldDefinition { Name = "MACDHistogramSlope", DataType = "REAL" },
                new FieldDefinition { Name = "TrendStrength", DataType = "REAL" },
                new FieldDefinition { Name = "TrendDuration", DataType = "INTEGER" },
                new FieldDefinition { Name = "IsUptrend", DataType = "INTEGER" },
                new FieldDefinition { Name = "DistanceToNearestSupport", DataType = "REAL" },
                new FieldDefinition { Name = "DistanceToNearestResistance", DataType = "REAL" },
                new FieldDefinition { Name = "DayOfWeek", DataType = "INTEGER" },
                new FieldDefinition { Name = "ADLChange", DataType = "REAL" },
                new FieldDefinition { Name = "HistoricalVolatility", DataType = "REAL" },
                new FieldDefinition { Name = "ROC_5", DataType = "REAL" },
                new FieldDefinition { Name = "ROC_10", DataType = "REAL" },
                new FieldDefinition { Name = "VWAP", DataType = "REAL" },
                new FieldDefinition { Name = "CompositeSentiment", DataType = "REAL" },
                new FieldDefinition { Name = "SentimentCategory", DataType = "TEXT" },
                new FieldDefinition { Name = "CompositeMomentum", DataType = "REAL" },
                new FieldDefinition { Name = "MACDCrossover", DataType = "INTEGER" },
                new FieldDefinition { Name = "EMACrossover", DataType = "INTEGER" },
                new FieldDefinition { Name = "CycleDominantPeriod", DataType = "INTEGER" },
                new FieldDefinition { Name = "CyclePhase", DataType = "REAL" },
                new FieldDefinition { Name = "IsBullishCyclePhase", DataType = "INTEGER" },
                new FieldDefinition { Name = "DynamicSupportLevel", DataType = "REAL" },
                new FieldDefinition { Name = "DynamicResistanceLevel", DataType = "REAL" },
                new FieldDefinition { Name = "DistanceToDynamicSupport", DataType = "REAL" },
                new FieldDefinition { Name = "DistanceToDynamicResistance", DataType = "REAL" },
                new FieldDefinition { Name = "FractalDimension", DataType = "REAL" },
                new FieldDefinition { Name = "HurstExponent", DataType = "REAL" },
                new FieldDefinition { Name = "MarketEfficiencyRatio", DataType = "REAL" },
                new FieldDefinition { Name = "MarketRegime", DataType = "TEXT" },
                new FieldDefinition { Name = "MarketVolatility", DataType = "REAL" },
                new FieldDefinition { Name = "OrderFlowImbalance", DataType = "REAL" },
                new FieldDefinition { Name = "RSIDivergence", DataType = "INTEGER" },
                new FieldDefinition { Name = "MACDDivergence", DataType = "INTEGER" },
                new FieldDefinition { Name = "PriceActionPattern", DataType = "TEXT" },
                new FieldDefinition { Name = "RSIDivergenceStrength", DataType = "REAL" },
                new FieldDefinition { Name = "VolatilityRegime", DataType = "TEXT" },
                new FieldDefinition { Name = "CandlePattern", DataType = "TEXT" },
                new FieldDefinition { Name = "CandlePatternRank", DataType = "INTEGER" },
                new FieldDefinition { Name = "CandlePatternScaledRank", DataType = "INTEGER" },
                new FieldDefinition { Name = "CandlePatternMatchCount", DataType = "INTEGER" },
                new FieldDefinition { Name = "BuySignal", DataType = "INTEGER" } // Target field for ML prediction
            };
        }
    }
}
