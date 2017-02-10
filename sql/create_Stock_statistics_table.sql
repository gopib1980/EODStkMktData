use stocks;

-- create an external hive table to manage the statistics.

create external table if not exists stock_statistics 
       ( symbol        string, 
         day_range     string,
         volume        bigint, 
         4wk_range     string,
         total_volume  bigint,
         avg_30_volume bigint,
         avg_10_volume bigint, 
         chg_30_volume decimal(9,2)
       )
       comment 'This is a statistics mined from Stock Exchange End data'
       partitioned by (xchange string,trade_date string) 
       row format delimited 
       fields terminated by "," 
       lines terminated by "\n" 
       null defined as "*"
       location '/data/processed/stock_statistics/'
       ;

