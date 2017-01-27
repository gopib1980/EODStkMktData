#!/bin/sh

# This script loads the EOD data from the stock exchanges which are downloaded from EODData.com

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

echo "\nStarted loading the files for Exchange" "$1" 

if [ "$1" = "NYSE" ]; then
	hive -f /projects/EODStkMktData/sql/dataLoad_NYSE.sql
fi

if [ "$1" = "NASDAQ" ]; then
	hive -f /projects/EODStkMktData/sql/dataLoad_NASDAQ.sql
fi

rc=$?;echo "Exit code is $rc"; if [ "$rc" != 0 ]; then exit $rc; fi

hadoop fs -rm -r -skipTrash '/data/EODMktData/*'

echo "Completed loading the data for $1 exchange" 
