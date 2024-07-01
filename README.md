# Crypto Candle Metrics Processor

Crypto Candle Metrics Processor is a tool for importing, analyzing, and exporting cryptocurrency candle data. It includes functionality for calculating various technical indicators and exporting the data to CSV files.

## Features

- **Database Creation**: Create a SQLite database and define the schema for storing candle data.
- **CSV Import**: Import candle data from CSV files into the database.
- **Technical Analysis**: Calculate various technical indicators such as SMA, EMA, ATR, RSI, MACD, Bollinger Bands, etc.
- **CSV Export**: Export the processed candle data with calculated indicators to CSV files.

## Dependencies

- CsvHelper
- Math.Net.Numerics
- Microsoft.Data.Sqlite
- Skender.Stock.Indicators


## Technical Indicators

The Crypto Candle Metrics Processor includes a comprehensive set of technical indicators to aid in the analysis of cryptocurrency candle data. Each indicator is calculated using specific formulas and updated in the SQLite database.


### 1. Accumulation/Distribution Line Change (ADLChangeIndicator)
**Description:** The ADLChangeIndicator calculates the change in the Accumulation/Distribution Line (ADL) between periods. It measures the net accumulation or distribution of a security over a specified time frame.

**Calculation:**
- **ADL Change:** \( \text{ADL Change} = \text{Current ADL} - \text{Previous ADL} \)

**Database Fields Updated:**
- **ADLChange:** The change in the ADL value between the current and previous periods.

### 2. Accumulation/Distribution Line (AdLineIndicator)
**Description:** The AdLineIndicator calculates the Accumulation/Distribution Line (ADL), which combines price and volume to indicate the flow of money into or out of a security.

**Calculation:**
- **ADL:** \( \text{ADL} = \sum \left( \frac{(\text{Close} - \text{Low}) - (\text{High} - \text{Close})}{\text{High} - \text{Low}} \times \text{Volume} \right) \)

**Database Fields Updated:**
- **ADL:** The Accumulation/Distribution Line value for each period.

### 3. Average Directional Index (ADX) Indicator (AdxIndicator)
**Description:** The AdxIndicator calculates the Average Directional Index (ADX), which measures the strength of a trend. It helps determine whether a market is trending or not.

**Calculation:**
- **ADX:** Calculated using the true range, directional movement indicators (DI+ and DI-), and a smoothing function.

**Database Fields Updated:**
- **ADX:** The Average Directional Index value for each period.

### 4. Average True Range (ATR) Indicator (AtrIndicator)
**Description:** The AtrIndicator calculates the Average True Range (ATR), which measures market volatility by decomposing the entire range of an asset price for a given period.

**Calculation:**
- **ATR:** The average of the true ranges over a specified period.

**Database Fields Updated:**
- **ATR:** The Average True Range value for each period.

### 5. ATR Percent Indicator (ATRPercentIndicator)
**Description:** The ATRPercentIndicator calculates the ATR as a percentage of the closing price. It normalizes the ATR value for better comparison across different securities.

**Calculation:**
- **ATR Percent:** \( \text{ATR Percent} = \frac{\text{ATR}}{\text{Close}} \)

**Database Fields Updated:**
- **ATRPercent:** The ATR value as a percentage of the closing price for each period.

### 6. Bollinger Bands Indicator (BollingerBandsIndicator)
**Description:** The BollingerBandsIndicator calculates Bollinger Bands, which are volatility bands placed above and below a moving average. They help identify overbought and oversold conditions.

**Calculation:**
- **Middle Band (SMA):** The simple moving average over a specified period.
- **Upper Band:** \( \text{SMA} + ( \text{Standard Deviation} \times \text{Multiplier}) \)
- **Lower Band:** \( \text{SMA} - ( \text{Standard Deviation} \times \text{Multiplier}) \)
- **Percent B:** \( \frac{\text{Close} - \text{Lower Band}}{\text{Upper Band} - \text{Lower Band}} \)
- **Z-Score:** \( \frac{\text{Close} - \text{SMA}}{\text{Standard Deviation}} \)
- **Band Width:** \( \frac{\text{Upper Band} - \text{Lower Band}}{\text{SMA}} \)

**Database Fields Updated:**
- **BB_SMA:** The simple moving average (middle band).
- **BB_UpperBand:** The upper Bollinger Band.
- **BB_LowerBand:** The lower Bollinger Band.
- **BB_PercentB:** The percentage of the closing price within the bands.
- **BB_ZScore:** The z-score of the closing price.
- **BB_Width:** The width of the Bollinger Bands.

### 7. Buy Signal Indicator (BuySignalIndicator)
**Description:** The BuySignalIndicator calculates and updates buy signals based on predefined rules using multiple indicators. It combines several indicators to generate a buy score and signals based on thresholds.

**Calculation:**
- **Buy Score:** Aggregates scores from various indicators such as RSI, Stochastic, ADX, Bollinger Percent B, CMF, MACD, ADL, EMA, SMA, Volume, and ATR.
- **Buy Signal:** Generated if the Buy Score meets or exceeds a specified threshold.

**Database Fields Updated:**
- **BuyScore:** The cumulative score based on multiple indicators.
- **BuySignal:** The final buy signal derived from the Buy Score.

### 8. Candle Pattern Indicator (CandlePatternIndicator)
**Description:** The CandlePatternIndicator identifies specific candlestick patterns that are often used in technical analysis to predict market movements. It ranks the detected patterns based on predefined rankings.

**Calculation:**
- **Pattern Recognition:** Various candle pattern functions (e.g., CDL3LINESTRIKE, CDLINVERTEDHAMMER) are used to detect patterns.
- **Best Pattern:** The pattern with the highest rank is selected.
- **Pattern Rank:** The rank of the detected pattern based on a predefined list.
- **Pattern Match Count:** The number of patterns detected for each candle.

**Database Fields Updated:**
- **CandlePattern:** The name of the detected candlestick pattern.
- **CandlePatternRank:** The rank of the detected pattern.
- **CandlePatternMatchCount:** The count of matching patterns.

### 9. Commodity Channel Index (CCI) Indicator (CciIndicator)
**Description:** The CciIndicator calculates the Commodity Channel Index (CCI), which is used to identify cyclical trends in a security.

**Calculation:**
- **CCI:** \( \text{CCI} = \frac{(\text{Typical Price} - \text{SMA})}{0.015 \times \text{Mean Deviation}} \)

**Database Fields Updated:**
- **CCI:** The Commodity Channel Index value for each period.

### 10. Chaikin Money Flow (CMF) Indicator (CmfIndicator)
**Description:** The CmfIndicator calculates the Chaikin Money Flow (CMF), which measures the accumulation and distribution of a security over a specified period.

**Calculation:**
- **CMF:** The sum of the Money Flow Volume over the period divided by the sum of the volume over the period.

**Database Fields Updated:**
- **CMF:** The Chaikin Money Flow value for each period.

### 11. Composite Market Sentiment (CompositeMarketSentimentIndicator)
**Description:** The CompositeMarketSentimentIndicator aggregates multiple sentiment indicators to provide a comprehensive measure of market sentiment.

**Calculation:**
- **Composite Sentiment:** Aggregates values from RSI, MACD Histogram, Bollinger Percent B, ADX, and Relative Volume.
- **Sentiment Category:** Categorizes sentiment as Very Bullish, Bullish, Neutral, Bearish, or Very Bearish based on the composite sentiment score.

**Database Fields Updated:**
- **CompositeSentiment:** The composite sentiment score.
- **SentimentCategory:** The category of the sentiment based on the score.

### 12. Composite Momentum Indicator (CompositeMomentumIndicator)
**Description:** The CompositeMomentumIndicator aggregates multiple momentum indicators to provide a comprehensive measure of market momentum.

**Calculation:**
- **Composite Momentum:** Aggregates values from RSI, MACD, and ROC.

**Database Fields Updated:**
- **CompositeMomentum:** The composite momentum score.

### 13. Crossover Features Indicator (CrossoverFeaturesIndicator)
**Description:** The CrossoverFeaturesIndicator detects crossover events between various moving averages and oscillators.

**Calculation:**
- **MACD Crossover:** Indicates if there is a crossover in the MACD and signal lines.
- **EMA Crossover:** Indicates if there is a crossover between short-term and long-term EMAs.

**Database Fields Updated:**
- **MACDCrossover:** Binary indicator for MACD crossover.
- **EMACrossover:** Binary indicator for EMA crossover.

### 14. Cyclical Patterns Indicator (CyclicalPatternsIndicator)
**Description:** The CyclicalPatternsIndicator identifies cyclical patterns in the price data.

**Calculation:**
- **Dominant Period:** The dominant cycle period in the data.
- **Phase:** The phase of the dominant cycle.
- **Is Bullish Phase:** Indicates if the current phase is bullish.

**Database Fields Updated:**
- **CycleDominantPeriod:** The dominant cycle period.
- **CyclePhase:** The phase of the cycle.
- **IsBullishCyclePhase:** Binary indicator for bullish cycle phase.

### 15. Day of Week Indicator (DayOfWeekIndicator)
**Description:** The DayOfWeekIndicator calculates the day of the week for each candle.

**Calculation:**
- **Day of Week:** Extracts the day of the week from the candle's date.

**Database Fields Updated:**
- **DayOfWeek:** The day of the week (0-6) for each candle.

### 16. Distance to Support/Resistance Indicator (DistanceToSupportResistanceIndicator)
**Description:** The DistanceToSupportResistanceIndicator calculates the distance to the nearest support and resistance levels.

**Calculation:**
- **Distance to Nearest Support:** \( \text{DistanceToNearestSupport} = \frac{\text{Close} - \text{Support1}}{\text{Close}} \)
- **Distance to Nearest Resistance:** \( \text{DistanceToNearestResistance} = \frac{\text{Resistance1} - \text{Close}}{\text{Close}} \)

**Database Fields Updated:**
- **DistanceToNearestSupport:** The distance to the nearest support level.
- **DistanceToNearestResistance:** The distance to the nearest resistance level.

### 17. Dynamic Support/Resistance Indicator (DynamicSupportResistanceIndicator)
**Description:** The DynamicSupportResistanceIndicator calculates dynamic support and resistance levels based on recent candle data.

**Calculation:**
- **Support Level:** The minimum low value from recent candles.
- **Resistance Level:** The maximum high value from recent candles.
- **Distance to Dynamic Support:** \( \text{DistanceToDynamicSupport} = \frac{\text{Close} - \text{SupportLevel}}{\text{Close}} \)
- **Distance to Dynamic Resistance:** \( \text{DistanceToDynamicResistance} = \frac{\text{ResistanceLevel} - \text{Close}}{\text{Close}} \)

**Database Fields Updated:**
- **DynamicSupportLevel:** The dynamic support level.
- **DynamicResistanceLevel:** The dynamic resistance level.
- **DistanceToDynamicSupport:** The distance to the dynamic support level.
- **DistanceToDynamicResistance:** The distance to the dynamic resistance level.

### 18. Exponential Moving Average (EMA) Indicator (EmaIndicator)
**Description:** The EmaIndicator calculates the Exponential Moving Average (EMA) for a specified period.

**Calculation:**
- **EMA:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **EMA:** The Exponential Moving Average value for each period.

### 19. Fibonacci Retracement Indicator (FibonacciRetracementIndicator)
**Description:** The FibonacciRetracementIndicator calculates Fibonacci retracement levels for the specified period.

**Calculation:**
- **Fibonacci Levels:** Calculated as percentages (23.6%, 38.2%, 50%, 61.8%, 78.6%) of the range between the highest high and lowest low.

**Database Fields Updated:**
- **FibRetracement_23_6:** The 23.6% Fibonacci retracement level.
- **FibRetracement_38_2:** The 38.2% Fibonacci retracement level.
- **FibRetracement_50:** The 50% Fibonacci retracement level.
- **FibRetracement_61_8:** The 61.8% Fibonacci retracement level.
- **FibRetracement_78_6:** The 78.6% Fibonacci retracement level.

### 20. Fractal Dimension Indicator (FractalDimensionIndicator)
**Description:** The FractalDimensionIndicator calculates the fractal dimension of the price series, providing a measure of market complexity.

**Calculation:**
- **Fractal Dimension:** Calculated using the Higuchi method.

**Database Fields Updated:**
- **FractalDimension:** The fractal dimension value for each period.


### 21. Historical Volatility Indicator (HistoricalVolatilityIndicator)
**Description:** The HistoricalVolatilityIndicator calculates the historical volatility of the price over a specified period.

**Calculation:**
- **Historical Volatility:** 
  \[
  \text{Returns} = \ln\left(\frac{\text{Close}_t}{\text{Close}_{t-1}}\right)
  \]
  \[
  \text{Standard Deviation} = \sqrt{\frac{\sum (\text{Returns} - \text{Mean Return})^2}{\text{Period} - 1}}
  \]
  \[
  \text{Annualized Volatility} = \text{Standard Deviation} \times \sqrt{252}
  \]

**Database Fields Updated:**
- **HistoricalVolatility:** The annualized historical volatility value.

### 22. Hurst Exponent Indicator (HurstExponent)
**Description:** The HurstExponent calculates the Hurst exponent to measure the long-term memory of a time series.

**Calculation:**
- **Hurst Exponent:** 
  \[
  \text{R/S Analysis}
  \]
  Perform regression on the log of R/S values against the log of lags to find the slope, which is the Hurst exponent.

**Database Fields Updated:**
- **HurstExponent:** The Hurst exponent value.

### 23. Ichimoku Indicator (IchimokuIndicator)
**Description:** The IchimokuIndicator calculates the Ichimoku Cloud components.

**Calculation:**
- **Tenkan-sen (Conversion Line):**
  \[
  \text{Tenkan-sen} = \frac{\text{Highest High} + \text{Lowest Low}}{2} \text{ (over the last 9 periods)}
  \]
- **Kijun-sen (Base Line):**
  \[
  \text{Kijun-sen} = \frac{\text{Highest High} + \text{Lowest Low}}{2} \text{ (over the last 26 periods)}
  \]
- **Senkou Span A (Leading Span A):**
  \[
  \text{Senkou Span A} = \frac{\text{Tenkan-sen} + \text{Kijun-sen}}{2} \text{ (plotted 26 periods ahead)}
  \]
- **Senkou Span B (Leading Span B):**
  \[
  \text{Senkou Span B} = \frac{\text{Highest High} + \text{Lowest Low}}{2} \text{ (over the last 52 periods, plotted 26 periods ahead)}
  \]
- **Chikou Span (Lagging Span):**
  \[
  \text{Chikou Span} = \text{Close price plotted 26 periods back}
  \]

**Database Fields Updated:**
- **Ichimoku_TenkanSen:** The Tenkan-sen value.
- **Ichimoku_KijunSen:** The Kijun-sen value.
- **Ichimoku_SenkouSpanA:** The Senkou Span A value.
- **Ichimoku_SenkouSpanB:** The Senkou Span B value.
- **Ichimoku_ChikouSpan:** The Chikou Span value.

### 24. Lagged Features Indicator (LaggedFeaturesIndicator)
**Description:** The LaggedFeaturesIndicator calculates lagged features for various indicators and updates the database.

**Calculation:**
- **Lagged Closing Prices:**
  \[
  \text{LaggedClose}_n = \text{Close Price of } (t-n)
  \]
- **Lagged RSI, EMA, ATR, MACD, Bollinger Bands, Stochastic Oscillator:**
  \[
  \text{LaggedRSI}_n = \text{RSI of } (t-n)
  \]
  \[
  \text{LaggedEMA}_n = \text{EMA of } (t-n)
  \]
  \[
  \text{LaggedATR}_n = \text{ATR of } (t-n)
  \]
  \[
  \text{LaggedMACD}_n = \text{MACD of } (t-n)
  \]
  \[
  \text{LaggedBollingerUpper}_n = \text{Upper Band of } (t-n)
  \]
  \[
  \text{LaggedBollingerLower}_n = \text{Lower Band of } (t-n)
  \]
  \[
  \text{LaggedStochK}_n = \text{Stochastic %K of } (t-n)
  \]
  \[
  \text{LaggedStochD}_n = \text{Stochastic %D of } (t-n)
  \]

**Database Fields Updated:**
- **Lagged_Close_1, Lagged_Close_2, Lagged_Close_3:** Lagged close prices for 1, 2, and 3 periods back.
- **Lagged_RSI_1, Lagged_RSI_2, Lagged_RSI_3:** Lagged RSI values for 1, 2, and 3 periods back.
- **Lagged_Return_1, Lagged_Return_2, Lagged_Return_3:** Lagged returns for 1, 2, and 3 periods back.
- **Lagged_EMA_1, Lagged_EMA_2, Lagged_EMA_3:** Lagged EMA values for 1, 2, and 3 periods back.
- **Lagged_ATR_1, Lagged_ATR_2, Lagged_ATR_3:** Lagged ATR values for 1, 2, and 3 periods back.
- **Lagged_MACD_1, Lagged_MACD_2, Lagged_MACD_3:** Lagged MACD values for 1, 2, and 3 periods back.
- **Lagged_BollingerUpper_1, Lagged_BollingerUpper_2, Lagged_BollingerUpper_3:** Lagged Bollinger Upper Band values for 1, 2, and 3 periods back.
- **Lagged_BollingerLower_1, Lagged_BollingerLower_2, Lagged_BollingerLower_3:** Lagged Bollinger Lower Band values for 1, 2, and 3 periods back.
- **Lagged_BollingerPercentB_1, Lagged_BollingerPercentB_2, Lagged_BollingerPercentB_3:** Lagged Bollinger PercentB values for 1, 2, and 3 periods back.
- **Lagged_StochK_1, Lagged_StochK_2, Lagged_StochK_3:** Lagged Stochastic %K values for 1, 2, and 3 periods back.
- **Lagged_StochD_1, Lagged_StochD_2, Lagged_StochD_3:** Lagged Stochastic %D values for 1, 2, and 3 periods back.


### 25. MACD Histogram Slope Indicator (MACDHistogramSlopeIndicator)
**Description:** The MACDHistogramSlopeIndicator calculates the slope of the MACD histogram between consecutive periods. It helps to identify changes in the momentum of the price movements.

**Calculation:**
- **MACD Histogram Slope:** 
  \[
  \text{MACD Histogram Slope} = \text{Current MACD Histogram} - \text{Previous MACD Histogram}
  \]

**Database Fields Updated:**
- **MACDHistogramSlope:** The slope of the MACD histogram between consecutive periods.

### 26. MACD Indicator (MacdIndicator)
**Description:** The MacdIndicator calculates the Moving Average Convergence Divergence (MACD) indicator, which is used to identify changes in the strength, direction, momentum, and duration of a trend.

**Calculation:**
- **MACD:** The difference between the 12-period EMA and the 26-period EMA.
- **Signal Line:** The 9-period EMA of the MACD.
- **MACD Histogram:** The difference between the MACD and the Signal Line.

**Database Fields Updated:**
- **MACD:** The MACD value.
- **MACD_Signal:** The signal line value.
- **MACD_Histogram:** The histogram value.

### 27. Market Efficiency Ratio (MarketEfficiencyRatio)
**Description:** The MarketEfficiencyRatio calculates the Market Efficiency Ratio (MER), which measures the efficiency of price movements over a specified period.

**Calculation:**
- **MER:** 
  \[
  \text{MER} = \frac{\text{Net Price Change}}{\sum \text{Price Changes}}
  \]

**Database Fields Updated:**
- **MarketEfficiencyRatio:** The Market Efficiency Ratio value for each period.

### 28. Market Regime Indicator (MarketRegimeIndicator)
**Description:** The MarketRegimeIndicator determines the market regime (e.g., trending, ranging) based on volatility and moving average.

**Calculation:**
- **Volatility:** 
  \[
  \text{Volatility} = \frac{\text{ATR}}{\text{SMA}}
  \]
- **Market Regime:** Determined based on volatility and price position relative to the SMA.

**Database Fields Updated:**
- **MarketRegime:** The market regime (e.g., Trending Up, Trending Down, Ranging, Transitioning).
- **MarketVolatility:** The volatility value.

### 29. On-Balance Volume (OBV) Indicator (ObvIndicator)
**Description:** The ObvIndicator calculates the On-Balance Volume (OBV), which measures buying and selling pressure as a cumulative indicator that adds volume on up days and subtracts volume on down days.

**Calculation:**
- **OBV:** 
  \[
  \text{OBV} = \text{Previous OBV} + 
  \begin{cases} 
  \text{Volume}, & \text{if Close} > \text{Previous Close} \\
  -\text{Volume}, & \text{if Close} < \text{Previous Close} \\
  0, & \text{if Close} = \text{Previous Close} 
  \end{cases}
  \]

**Database Fields Updated:**
- **OBV:** The On-Balance Volume value for each period.

### 30. Order Flow Imbalance Indicator (OrderFlowImbalanceIndicator)
**Description:** The OrderFlowImbalanceIndicator calculates the order flow imbalance, measuring the difference between buying and selling pressure over a specified period.

**Calculation:**
- **Order Flow Imbalance:** 
  \[
  \text{Order Flow Imbalance} = \frac{\text{Buying Pressure} - \text{Selling Pressure}}{\text{Buying Pressure} + \text{Selling Pressure}}
  \]

**Database Fields Updated:**
- **OrderFlowImbalance:** The order flow imbalance value for each period.

### 31. Oscillator Divergences Indicator (OscillatorDivergencesIndicator)
**Description:** The OscillatorDivergencesIndicator detects divergences between price movements and oscillator values such as RSI and MACD, which can signal potential reversals.

**Calculation:**
- **RSI Divergence:** Detected when price and RSI move in opposite directions.
- **MACD Divergence:** Detected when price and MACD move in opposite directions.

**Database Fields Updated:**
- **RSIDivergence:** Binary indicator for RSI divergence.
- **MACDDivergence:** Binary indicator for MACD divergence.


### 32. Parabolic SAR Indicator (ParabolicSarIndicator)
**Description:** The ParabolicSarIndicator calculates the Parabolic SAR (Stop and Reverse) indicator, which is used to determine the direction of an asset's momentum and the point in time when this momentum has a higher-than-normal probability of switching directions.

**Calculation:**
- **Parabolic SAR:** Calculated using the high, low, and close prices to determine the potential reversal points.

**Database Fields Updated:**
- **ParabolicSar:** The Parabolic SAR value for each period.

### 33. Price Action Classification (PriceActionClassification)
**Description:** The PriceActionClassification identifies the pattern of price action over a specified period and classifies it into categories such as Uptrend, Downtrend, Rangebound, or Mixed.

**Calculation:**
- **Price Action Pattern:** Classified based on the close and open prices of recent candles.

**Database Fields Updated:**
- **PriceActionPattern:** The classified price action pattern.

### 34. Price Change Percent Indicator (PriceChangePercentIndicator)
**Description:** The PriceChangePercentIndicator calculates the percentage change in price from the open to the close for each period.

**Calculation:**
- **Price Change Percent:** 
  \[
  \text{Price Change Percent} = \frac{\text{Close} - \text{Open}}{\text{Open}}
  \]

**Database Fields Updated:**
- **PriceChangePercent:** The percentage change in price for each period.

### 35. Price Position in Range Indicator (PricePositionInRangeIndicator)
**Description:** The PricePositionInRangeIndicator calculates the position of the close price within the high-low range of each period.

**Calculation:**
- **Price Position in Range:** 
  \[
  \text{Price Position in Range} = \frac{\text{Close} - \text{Low}}{\text{High} - \text{Low}}
  \]

**Database Fields Updated:**
- **PricePositionInRange:** The position of the close price within the high-low range.

### 36. Price Up Indicator (PriceUpIndicator)
**Description:** The PriceUpIndicator calculates whether the closing price is higher than the previous period's closing price.

**Calculation:**
- **Price Up:** Binary indicator (1 if the close price is higher than the previous period, 0 otherwise).

**Database Fields Updated:**
- **PriceUp:** Binary indicator for price increase.

### 37. Price Up Streak Indicator (PriceUpStreakIndicator)
**Description:** The PriceUpStreakIndicator calculates the streak of consecutive periods where the closing price is higher than the previous period.

**Calculation:**
- **Price Up Streak:** The number of consecutive periods with a higher closing price compared to the previous period.

**Database Fields Updated:**
- **PriceUpStreak:** The streak count of consecutive periods with a price increase.


### 38. Relative Volume Profile (RelativeVolumeProfile)
**Description:** The RelativeVolumeProfile calculates the relative volume compared to a rolling average and standard deviation over a specified lookback period. It also classifies the volume into categories such as Extremely High, High, Normal, Low, and Extremely Low.

**Calculation:**
- **Relative Volume:** 
  \[
  \text{Relative Volume} = \frac{\text{Current Volume} - \text{Average Volume}}{\text{Standard Deviation of Volume}}
  \]
- **Volume Profile:** Classification based on the relative volume.

**Database Fields Updated:**
- **RelativeVolume:** The relative volume value.
- **VolumeProfile:** The classified volume profile.

### 39. Rate of Change (ROC) Indicator (ROCIndicator)
**Description:** The ROCIndicator calculates the Rate of Change (ROC) for specified periods to measure the percentage change in price.

**Calculation:**
- **ROC:** 
  \[
  \text{ROC}_n = \frac{\text{Close} - \text{Close}_{n}}{\text{Close}_{n}}
  \]

**Database Fields Updated:**
- **ROC_5:** The ROC value for a 5-period lookback.
- **ROC_10:** The ROC value for a 10-period lookback.

### 40. Rolling Pivot Points Indicator (RollingPivotPointsIndicator)
**Description:** The RollingPivotPointsIndicator calculates pivot points based on a rolling window of periods to identify potential support and resistance levels.

**Calculation:**
- **Pivot Points:** Calculated using high, low, and close prices over a rolling window.

**Database Fields Updated:**
- **PivotPoint:** The pivot point value.
- **Resistance1, Resistance2, Resistance3:** The resistance levels.
- **Support1, Support2, Support3:** The support levels.

### 41. RSI Change Indicator (RSIChangeIndicator)
**Description:** The RSIChangeIndicator calculates the change in the Relative Strength Index (RSI) value between periods.

**Calculation:**
- **RSI Change:** 
  \[
  \text{RSI Change} = \text{Current RSI} - \text{Previous RSI}
  \]

**Database Fields Updated:**
- **RSIChange:** The change in RSI value between the current and previous periods.

### 42. RSI Divergence Strength Indicator (RSIDivergenceStrengthIndicator)
**Description:** The RSIDivergenceStrengthIndicator calculates the strength of divergence between price changes and RSI changes.

**Calculation:**
- **RSI Divergence Strength:** 
  \[
  \text{Divergence Strength} = \left| \text{Price Change} - \text{RSI Change} \right| \quad \text{if signs differ}
  \]

**Database Fields Updated:**
- **RSIDivergenceStrength:** The strength of the RSI divergence.

### 43. Relative Strength Index (RSI) Indicator (RsiIndicator)
**Description:** The RsiIndicator calculates the Relative Strength Index (RSI) for a specified period, which measures the magnitude of recent price changes to evaluate overbought or oversold conditions.

**Calculation:**
- **RSI:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **RSI:** The Relative Strength Index value for each period.


### 44. Simple Moving Average (SMA) Indicator (SmaIndicator)
**Description:** The SmaIndicator calculates the Simple Moving Average (SMA) for a specified period.

**Calculation:**
- **SMA:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **SMA:** The Simple Moving Average value for each period.

### 45. Statistical Indicators (StatisticalIndicators)
**Description:** The StatisticalIndicators calculates various statistical metrics over a rolling window.

**Calculation:**
- **Rolling Mean:** 
  \[
  \text{Rolling Mean} = \text{Average of Close Prices over the Period}
  \]
- **Rolling Standard Deviation:** 
  \[
  \text{Rolling Std Dev} = \sqrt{\text{Variance of Close Prices over the Period}}
  \]
- **Rolling Variance:** 
  \[
  \text{Rolling Variance} = \text{Variance of Close Prices over the Period}
  \]
- **Rolling Skewness:** 
  \[
  \text{Rolling Skewness} = \text{Skewness of Close Prices over the Period}
  \]
- **Rolling Kurtosis:** 
  \[
  \text{Rolling Kurtosis} = \text{Kurtosis of Close Prices over the Period}
  \]

**Database Fields Updated:**
- **RollingMean:** The rolling mean value.
- **RollingStdDev:** The rolling standard deviation value.
- **RollingVariance:** The rolling variance value.
- **RollingSkewness:** The rolling skewness value.
- **RollingKurtosis:** The rolling kurtosis value.

### 46. Stochastic Oscillator Indicator (StochasticIndicator)
**Description:** The StochasticIndicator calculates the Stochastic Oscillator, which indicates overbought or oversold conditions.

**Calculation:**
- **%K and %D:**
  \[
  \text{Stoch}_K = \frac{\text{Current Close} - \text{Lowest Low}}{\text{Highest High} - \text{Lowest Low}} \times 100
  \]
  \[
  \text{Stoch}_D = \text{SMA of } \text{Stoch}_K
  \]

**Database Fields Updated:**
- **Stoch_K:** The %K value of the Stochastic Oscillator.
- **Stoch_D:** The %D value of the Stochastic Oscillator.

### 47. SuperTrend Indicator (SupertrendIndicator)
**Description:** The SupertrendIndicator calculates the SuperTrend, which helps identify the direction of the trend.

**Calculation:**
- **SuperTrend:** Calculated using the Average True Range (ATR) and a multiplier.

**Database Fields Updated:**
- **SuperTrend:** The SuperTrend value.

### 48. Triple Exponential Moving Average (TEMA) Indicator (TemaIndicator)
**Description:** The TemaIndicator calculates the Triple Exponential Moving Average (TEMA) for a specified period.

**Calculation:**
- **TEMA:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **TEMA:** The Triple Exponential Moving Average value for each period.


### 49. Trend Strength Indicator (TrendStrengthIndicator)
**Description:** The TrendStrengthIndicator calculates the trend strength and duration using ADX and SMA values.

**Calculation:**
- **ADX Value:** The ADX value for trend strength.
- **Price to SMA:** The ratio of the current price to the SMA value.
- **Trend Strength:** \( \text{TrendStrength} = \text{ADX} \times (\text{Price to SMA} > 1 ? \text{Price to SMA} : 1 / \text{Price to SMA}) \)
- **Trend Duration:** The duration of the current trend.
- **Is Uptrend:** A boolean indicating if the trend is an uptrend.

**Database Fields Updated:**
- **TrendStrength:** The calculated trend strength value.
- **TrendDuration:** The duration of the current trend.
- **IsUptrend:** A boolean indicating if the trend is an uptrend.

### 50. Volatility Regime Indicator (VolatilityRegime)
**Description:** The VolatilityRegime calculates the volatility regime based on the ATR and its standard deviation.

**Calculation:**
- **Mean ATR:** The average ATR over the specified period.
- **Standard Deviation of ATR:** The standard deviation of ATR over the specified period.
- **Volatility Regime:** Categorized as "High", "Medium", or "Low" based on the ATR value.

**Database Fields Updated:**
- **VolatilityRegime:** The categorized volatility regime.

### 51. Volume Change Percent Indicator (VolumeChangePercentIndicator)
**Description:** The VolumeChangePercentIndicator calculates the percentage change in volume compared to the previous period.

**Calculation:**
- **Volume Change Percent:** \( \text{VolumeChangePercent} = \frac{\text{Current Volume} - \text{Previous Volume}}{\text{Previous Volume}} \)

**Database Fields Updated:**
- **VolumeChangePercent:** The percentage change in volume for each period.

### 52. VWAP Indicator (VWAPIndicator)
**Description:** The VWAPIndicator calculates the Volume Weighted Average Price (VWAP).

**Calculation:**
- **VWAP:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **VWAP:** The Volume Weighted Average Price value for each period.

### 53. Williams %R Indicator (WilliamsRIndicator)
**Description:** The WilliamsRIndicator calculates the Williams %R, which is a momentum indicator measuring overbought and oversold levels.

**Calculation:**
- **Williams %R:** Calculated using the formula provided by the Skender.Stock.Indicators library.

**Database Fields Updated:**
- **WilliamsR:** The Williams %R value for each period.


