#!/bin/bash

PIG_QUERY_DIR=$SPARK_HOME/sql/core/src/test/resources/pigmix

for query in $PIG_QUERY_DIR/*; do
    echo -e "\nRunning query: $query\n"
    outputFile=mktemp
    queryText=$(cat $PIG_QUERY_DIR/$query)

    $SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.sql.PigOnSparkBenchmark \
    --master $SPARK_MASTER \
    /path/to/examples.jar \
    $queryText $outputFile 5

    echo -e "\nOutput for $query is in $outputFile\n"
done