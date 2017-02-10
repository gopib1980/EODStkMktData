use stocks;

-- load the data to main stock exchange table eod_stock_data
insert into table eod_stock_data 
        partition (xchange='NYSE')
        select symbol, to_date(from_unixtime(unix_timestamp(substr(tdate,0,11),'dd-MMM-yyyy')))  as trade_date, open, high, low, close,volume from eod_stock_data_landing;


