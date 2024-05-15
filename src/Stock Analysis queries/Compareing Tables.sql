
# Ques.1 which stock in each sector has the higest and lowest trading volume last month?

with max_vol as(

(select 'Tech' as sector, ticker, max(volume) as max_volume from `stockdata.Tech`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by max(volume) desc
limit 1)

UNION ALL

(select 'Finance' as sector, ticker, max(volume) as max_volume from `stockdata.finance`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by max(volume) desc
limit 1)

 UNION ALL

(select 'Healtcare' as sector, ticker, max(volume) as max_volume from `stockdata.healthcare`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by max(volume) desc
limit 1)), 

min_vol as (

(select 'Tech' as sector, ticker, min(volume) as min_volume from `stockdata.Tech`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by min(volume) 
limit 1)

UNION ALL

(select 'Finance' as sector, ticker, min(volume) as min_volume from `stockdata.finance`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by min(volume) 
limit 1)

 UNION ALL

(select 'Healtcare' as sector, ticker, min(volume) as min_volume from `stockdata.healthcare`
where extract(month from date)=extract(month from current_date())-1
group by ticker
order by min(volume) 
limit 1)
)

select a.sector, a.ticker, a.max_volume, b.ticker, b.min_volume from max_vol a join min_vol b
on a.sector=b.sector;



# Ques.2 what is  max price fluctuations (high - low) among given stocks in each sector daily basis?

with tech as(
  select 'Tech' as sector,date, max(round((high-low),2)) as tech_daily_fluctuation from `stockdata.Tech`
  group by date
), finance as (
   select 'Finance' as sector,date, max(round((high-low),2)) as fin_daily_fluctuation from `stockdata.finance`
  group by date
), Healthcare as (
   select 'Healthcare' as sector,date, max(round((high-low),2)) as health_daily_fluctuation from `stockdata.healthcare`
  group by date)

  select a.date, a.tech_daily_fluctuation, b.fin_daily_fluctuation, c.health_daily_fluctuation from 
  tech a join finance b on a.date=b.date join healthcare c on b.date=c.date;


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


with topstocks as(
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
order by price desc limit 1)), 
stocks as(
  select 'Tech' as sector, * from `stockdata.Tech`
  union all
  select 'Finance' as sector, * from `stockdata.finance`
  union all
  select 'Healthcare' as sector, * from `stockdata.healthcare`
)

select b.sector,b.ticker, round(a.close,2) as closing_value, a.date from stocks a join TopStocks b on
a.ticker=b.ticker and a.sector=b.sector

