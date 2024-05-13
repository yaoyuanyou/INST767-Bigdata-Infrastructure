
# can you construct an SQL query to calculate the daily price range (difference between the high and low prices) for Tesla? 
# The query should include the date and price range, and results should be ordered chronologically by date
SELECT Date, High, Low, High - Low AS Price_Range
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Date;


# Can you write an SQL query to find the day with the lowest price range for Tesla uery should select all columns for Tesla's stock data, 
# sort it by the lowest price recorded, and return just the entry with the smallest value
SELECT *
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Low ASC
LIMIT 1;


#Can you create a SQL query that retrieves the record with the highest trading volume for Tesla 
#sort the results by trading volume in descending order, and return only the single day with the highest volume
SELECT *
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Volume DESC
LIMIT 1;

#Can you write an query to calculate the average closing price of Apple stock over a specific time period, such as monthly or quarterly?

SELECT FORMAT_TIMESTAMP('%Y-%m', Date) AS Month, AVG(Close) AS Average_Closing_Price
FROM stockdata.Tech
WHERE ticker = 'AAPL'
GROUP BY Month
ORDER BY Month;

#Could you create an SQL query that identifies days with significant price movements for Apple?
#The query should find days where the price change from open to close was above a set threshold, indicating large movements

SELECT Date, Open, Close, ABS(Close - Open) AS Price_Change
FROM stockdata.Tech
WHERE ticker = 'AAPL' AND ABS(Close - Open) > 5 -- Change '5' to adjust the sensitivity
ORDER BY Price_Change DESC;


#Can you develop an query that computes a 7-day moving average of the closing prices for Tesla?
# This would be used to smooth out daily price data to better understand underlying trends.
SELECT Date,
       AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS Moving_Average
FROM stockdata.Tech
WHERE ticker = 'TSLA'
ORDER BY Date;








