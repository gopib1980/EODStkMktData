-- create database
create database if not exists stocks 
       comment "This is a hive database for a POC project on stock price analysis application";

use stocks;

-- create the landing table eod_stock_data_landing

create external table if not exists eod_stock_data_landing
       (  symbol string, 
          tdate string, 
          open decimal(9,2), 
          high decimal(9,2), 
          low decimal(9,2), 
          close decimal(9,2), 
          volume bigint
       ) 
       comment 'Landing table for Stock Exchange Data'
       row format delimited 
       fields terminated by "," 
       lines terminated by "\n" 
       null defined as "*"
       location '/data/EODMktDataLanding/'
       ;

-- create the symbol list tables for NASDAQ

create external table if not exists stock_symbol_nasdaq
       (  symbol  string,
          company string
       )
       comment 'NASDAQ symbol list'
       row format delimited 
       fields terminated by "\t" lines 
       terminated by "\n" 
       null defined as "*"
       location '/data/symbols/nasdaq/'
       ;

analyze table stock_symbol_nasdaq compute statistics for columns;

-- create the symbol list tables for NYSE

create external table if not exists stock_symbol_nyse
       (  symbol  string,
          company string
       )
       comment 'NASDAQ symbol list'
       row format delimited 
       fields terminated by "\t" lines 
       terminated by "\n" 
       null defined as "*"
       location '/data/symbols/nyse/'
       ;

analyze table stock_symbol_nyse compute statistics for columns;

-- create the master symbol view
create view if not exists stock_symbols as
       select 'NASDAQ' as xchange,symbol,company from stock_symbol_nasdaq
       union all
       select 'NYSE'   as xchange,symbol,company from stock_symbol_nyse
       ;

-- create a primary key index symbol view
--alter view stock_symbols add constraint symbols_pk primary key (xchange,symbol) disable nonvalidate;

-- create the master eod_stock_data partitioned table

create table if not exists eod_stock_data
       (  symbol string, 
          trade_date date, 
          open decimal(9,2), 
          high decimal(9,2), 
          low decimal(9,2), 
          close decimal(9,2), 
          volume bigint
       ) 
       comment 'Stock Exchange End of Day Table'
       partitioned by (xchange string) 
       row format delimited 
       fields terminated by "," 
       lines terminated by "\n" 
       null defined as "*"
       ;

--alter table eod_stock_data add constraint symbols_pk primary key (symbol) disable nonvalidate;
--alter table eod_stock_data add constraint symbols_fk foreign key (xchange,symbol) references stock_symbols(xchange,symbol);


