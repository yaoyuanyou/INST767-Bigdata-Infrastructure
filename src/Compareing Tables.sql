
# Ques.1 Which sector had the highest average trading volume last month?

select sector, Avg_Volume as Max_avg_stock_value from (
SELECT 'Tech' AS Sector, round(AVG(Volume),2) AS Avg_Volume FROM stockdata.Tech
UNION ALL
SELECT 'Finance', round(AVG(Volume),2) FROM stockdata.finance
UNION ALL
SELECT 'Healthcare', round(AVG(Volume),2) FROM stockdata.healthcare
WHERE EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM CURRENT_DATE()) - 1
ORDER BY Avg_Volume DESC
LIMIT 1);



# Ques.2 How do the maximum daily price fluctuations (high - low) compare across sectors?

SELECT Sector, round(MAX(Price_Fluctuation),2) AS Max_Fluctuation
FROM (
  SELECT 'Tech' AS Sector, High - Low AS Price_Fluctuation FROM stockdata.Tech
  UNION ALL
  SELECT 'Finance', High - Low FROM stockdata.finance
  UNION ALL
  SELECT 'Healthcare', High - Low FROM stockdata.healthcare
)
GROUP BY Sector;


# Ques.3 What are the top stock from each sector based on the past 30 days ?

(SELECT 'Tech' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.Tech a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1)

UNION ALL

(SELECT 'Finance' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.finance a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1)

UNION ALL

(SELECT 'Healthcare' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.healthcare a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1);



# Ques.4 What are the trends in closing prices for the top stock from each sector over past 30 days?

# using previous query as reference for this question to get top stocks



WITH TopStocks AS (
  
(SELECT 'Tech' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.Tech a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1)

UNION ALL

(SELECT 'Finance' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.finance a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1)

UNION ALL

(SELECT 'Healthcare' AS Sector, a.ticker, round(max(a.average),2) as price FROM  stockdata.healthcare a
where a.Date<=current_date() and a.Date>=(current_date()-30)
group by a.ticker
order by price desc limit 1)
)

(SELECT s.Sector, t.Date, round(t.Close,2) as close
FROM TopStocks s
JOIN stockdata.Tech t ON t.ticker = s.ticker 
where t.Date<=current_date() and t.Date>=(current_date()-30)
order by t.Date)

UNION ALL

(SELECT s.Sector, f.Date, round(f.Close,2) as close
FROM TopStocks s
JOIN stockdata.finance f ON f.ticker = s.ticker
where f.Date<=current_date() and f.Date>=(current_date()-30)
order by f.Date)

UNION ALL

(SELECT s.Sector, h.Date, round(h.Close,2) as close
FROM TopStocks s
JOIN stockdata.healthcare h ON h.ticker = s.ticker 
where h.Date<=current_date() and h.Date>=(current_date()-30)
order by h.date);

