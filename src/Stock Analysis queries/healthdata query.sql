

# Ques What is the daily price range for each healthcare stock, and how does it provide insights into the daily volatility of these stocks?

SELECT Date, ticker, High - Low AS Daily_Fluctuation
FROM  stockdata.healthcare
WHERE ticker IN ('LLY', 'MRK', 'ABT', 'ABBV')
ORDER BY Date;


# Ques. What is the average trading volume per month for each stock within the Tech, Finance, and Healthcare sectors,  and how does this reflect the overall trading activity for these stocks over time?

SELECT 
    ticker,
    EXTRACT(YEAR FROM Date) AS Year,
    EXTRACT(MONTH FROM Date) AS Month,
    AVG(Volume) AS Average_Volume
FROM stockdata.healthcare
WHERE ticker IN ('LLY', 'MRK', 'ABT', 'ABBV')
GROUP BY ticker, Year, Month
ORDER BY ticker, Year, Month;


# Ques. What is the highest closing price ever recorded for each of the healthcare companies like LLY, MRK, ABT, and ABBV within our dataset?

SELECT ticker, MAX(Close) AS Highest_Closing_Price
FROM stockdata.healthcare
WHERE ticker IN ('LLY', 'MRK', 'ABT', 'ABBV')
GROUP BY ticker
ORDER BY Highest_Closing_Price DESC;



# Ques. How have the closing prices of key stocks in the Tech, Finance, and Healthcare sectors trended over the past year?
SELECT Date, ticker, Close
FROM stockdata.healthcare
WHERE ticker IN ('LLY', 'MRK', 'ABT', 'ABBV')
ORDER BY Date, ticker;


# Ques. How did the performance of key healthcare stocks such as LLY, MRK, ABT, and ABBV differ on specific days marked by significant market 
# movements or external events impacting the healthcare sector?
SELECT Date, ticker, Open, Close, (Close - Open) / Open * 100 AS Percent_Change
FROM stockdata.healthcare
WHERE ticker IN ('LLY', 'MRK', 'ABT', 'ABBV') AND Date IN ('2024-04-01', '2024-04-15') 
ORDER BY Date, ticker;
