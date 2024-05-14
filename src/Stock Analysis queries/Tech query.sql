
# Ques. the daily price range (difference between the high and low prices) for Tesla? 
SELECT Date, High, Low, High - Low AS Price_Range
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Date;



# Ques. What is the highest trading volume for Tesla 
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Volume DESC
LIMIT 1;

# Ques. calculate the average closing price of Apple stock monthly?

SELECT FORMAT_TIMESTAMP('%Y-%m', Date) AS Month, AVG(Close) AS Average_Closing_Price
FROM stockdata.Tech
WHERE ticker = 'AAPL'
GROUP BY Month
ORDER BY Month;

# Ques identify the days with significant price movements for Apple.

SELECT Date, Open, Close, ABS(Close - Open) AS Price_Change
FROM stockdata.Tech
WHERE ticker = 'AAPL' AND ABS(Close - Open) > 5 -- Change '5' to adjust the sensitivity
ORDER BY Price_Change DESC;






