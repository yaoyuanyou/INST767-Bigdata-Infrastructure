

#How do the average closing prices of selected stocks compare over a specific period?

SELECT ticker, AVG(Close) AS Average_Closing_Price
FROM stockdata.finance
WHERE ticker IN ('BAC', 'JPM', 'WFC', 'HSBC')
GROUP BY ticker
ORDER BY Average_Closing_Price DESC;


# Which days experienced the highest trading volume for each stock in our dataset, 
# and what might these peaks in trading activity suggest about significant events or market announcements affecting these stocks?

SELECT ticker, Date, Volume
FROM stockdata.finance
WHERE ticker IN ('BAC', 'JPM', 'WFC', 'HSBC')
ORDER BY Volume DESC
LIMIT 5;


# How does the daily price volatility, measured as the average daily price range (difference between high and low prices), 
# vary for selected stocks over a specific time period?

SELECT ticker, AVG(High - Low) AS Average_Daily_Volatility
FROM stockdata.finance
WHERE ticker IN ('BAC', 'JPM', 'WFC', 'HSBC')
GROUP BY ticker
ORDER BY Average_Daily_Volatility DESC;


# Can you provide a monthly performance report for each stock listed in our database, detailing the opening price at the start of each month, 
# the closing price at the end of the month, and the net change over the period?
# lets try a hard query Finance Stok     CTE for life

WITH MonthlyData AS (
  SELECT 
    ticker,
    FORMAT_TIMESTAMP('%Y-%m', Date) AS Month,
    FIRST_VALUE(Open) OVER (PARTITION BY ticker, FORMAT_TIMESTAMP('%Y-%m', Date) ORDER BY Date ASC) AS Month_Open,
    LAST_VALUE(Close) OVER (PARTITION BY ticker, FORMAT_TIMESTAMP('%Y-%m', Date) ORDER BY Date DESC) AS Month_Close
  FROM stockdata.finance
  WHERE ticker IN ('BAC', 'JPM', 'WFC', 'HSBC')
)

SELECT 
  ticker,
  Month,
  AVG(Month_Open) AS Opening_Price, 
  AVG(Month_Close) AS Closing_Price,  
  AVG(Month_Close) - AVG(Month_Open) AS Net_Monthly_Change
FROM MonthlyData
GROUP BY ticker, Month
ORDER BY ticker, Month;



# write an SQL query that determines the highest closing price reached by each of the major BAC,JPM, WFC, HSBC?
# The query should filter for these specific tickers, calculate the maximum closing price for each, group the results by ticker, 
# and order them to easily identify which stock had the highest closing price
SELECT 
    ticker,
    MAX(Close) AS Highest_Closing_Price
FROM stockdata.finance
WHERE ticker IN ('BAC', 'JPM', 'WFC', 'HSBC')
GROUP BY ticker
ORDER BY Highest_Closing_Price DESC;


