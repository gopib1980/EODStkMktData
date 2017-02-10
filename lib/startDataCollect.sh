#!/bin/sh

MYPROJLIB=/projects/EODStkMktData

# This script triggers the flume service to run a flume agent to stream data into HDFS

flume-ng agent --conf $MYPROJLIB/flume_agents --conf-file $MYPROJLIB/flume_agents/EODMktData_agent --name EODAgent

rc=$?; if [ "$rc" != 0 ]; then echo "Non zero return code $rc"; exit $rc; fi
