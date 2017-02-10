#!/bin/sh

# This script loads the EOD data from the stock exchanges which are downloaded from EODData.com


## Parameter validation
if [ "$1" = "" ]; then
	echo "\nSyntax is dataLoad.sh <exchange name>"
	echo "Exiting..."
	exit
else 
  if [ "$1" != "NYSE" ] &&  [ "$1" != "NASDAQ" ]; then
	echo "\nOnly NYSE and NASDAQ exchanges are accepted"
	echo "Exiting..."
	exit
  fi
fi

# Determine the dates to run the App
if [ "$2" = "" ]; then
      #ydate=`date  +%Y-%m-%d --date="1 days ago"`
      echo "The trade date is required"; exit $rc;
else
      ydate=$2
fi

## commonly used paths
MYPROJLIB=/projects/EODStkMktData
MYDATALIB=/data/EODMktDataLanding

echo "****************************************************************"
echo "\nStarted loading $2 data for $1 exchange"

# Call the HiveQL scripts to load the raw data into partitioned table convert the string to date.
  
if [ "$1" = "NYSE" ]; then
	echo "Executing dataLoad_NYSE.sql"
	hive -silent -f $MYPROJLIB/sql/dataLoad_NYSE.sql
fi

if [ "$1" = "NASDAQ" ]; then
	echo "Executing dataLoad_NASDAQ.sql"
	hive -silent -f $MYPROJLIB/sql/dataLoad_NASDAQ.sql
fi

rc=$?; if [ "$rc" != 0 ]; then echo "Non zero return code $rc"; exit $rc; fi

`hadoop fs -rm -r -skipTrash $MYDATALIB/*`

echo "Completed loading $2 data for $1 exchange" 

# Call the Spark Scala code to process the market data and generate statistics
echo "\nStarted creating the statistics for $1 exchange" 

/usr/bin/spark-submit --class "EODDataMinerApp" --master yarn --deploy-mode client --executor-cores 2 $MYPROJLIB/target/scala-2.10/eoddataminer_2.10-1.0.jar /user/hive/warehouse/tradingdb.db/eod_stock_data $1 $ydate
 
rc=$?; if [ "$rc" != 0 ]; then echo "Non zero return code $rc"; exit $rc; fi

echo "Completed creating the statistics for $1 exchange"

echo "use stocks;alter table stock_statistics add partition(xchange='$1',trade_date='$ydate');" > $MYPROJLIB/sql/addPartition.sql

hive -silent -f $MYPROJLIB/sql/addPartition.sql

rc=$?; if [ "$rc" != 0 ]; then echo "Non zero return code $rc"; exit $rc; fi

echo "Added partition xchange='$1',trade_date='$ydate' to table stock_statistics"

echo "Exporting the statistics to external MySQL table"
# Export the calculated statistics to external database
`sqoop export \
      --connect "jdbc:mysql://myhpenvy:3306/gopidbp" \
      --username=gopib \
      --password-file=/user/gopib/mypwd.txt \
      --table stock_statistics \
      --export-dir /data/processed/stock_statistics/xchange=$1/trade_date=$ydate \
      --input-fields-terminated-by ',' \
      --input-lines-terminated-by '\n' \
      --optionally-enclosed-by "'" \
      --clear-staging-table  \
      --staging-table stock_statistics_staging`

echo "Completed exporting the statistics to external MySQL table"
