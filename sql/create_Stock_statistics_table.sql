use stocks;

-- create an external hive table to manage the statistics.
drop table stock_statistics ;

create external table if not exists stock_statistics 
       ( symbol        string,
         prev_close    decimal(9,2),
         open          decimal(9,2),
         close         decimal(9,2),
         day_range     string,
         volume        bigint, 
         52wk_range     string,
         avg_30D_volume bigint,
         avg_10D_volume bigint, 
         chg_52W_volume string,
         moving_200D_avg decimal(9,2),
         moving_50D_avg  decimal(9,2)
       )
       comment 'Statistics mined from Stock Exchange End of Day data'
       partitioned by (xchange string,trade_date string) 
       row format delimited 
       fields terminated by "," 
       lines terminated by "\n" 
       null defined as "*"
       location '/data/stock_statistics/'
       ;


