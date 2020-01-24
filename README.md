# udacity-spark-kafka-sf-crime-data-streaming-analytics

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Changing SparkSession's property parameters can and do have an impact to the overall performance of data ingestion / processing, particularly on the throughput and latency of the data (given the specific ways the data processing has been programmed / coded).

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

In general, more partitions allow work to be distributed among more workers. Fewer partitions on the other hand, allow work to be done in a larger chunks (and often quicker) [Reference](https://github.com/WinCanton/udacity-spark-kafka-sf-crime-data-streaming-analytics/blob/master/images/Progress_Report.png).

In this project specific implementation, by reducing the number of partitions, the throughput (`processedRowsPerSecond`) seemed to have increased by reducing the number of partitions used (`spark.sql.shuffle.partitions`). This may indicate that by keeping the data in a larger chunk allows the code to process data more efficiently and also minimising overhead associated with shuffling in the high partition / parallelism scenario.

Careful selection of the following configuration parameters: `spark.sql.shuffle.partitions`, `spark.default.parallelism`, and `maxOffsetsPerTrigger` seemed to allow better control over the performance of the data processing operations.


## Spark UI Snapshot
![Spark UI Snapshot](https://github.com/WinCanton/udacity-spark-kafka-sf-crime-data-streaming-analytics/blob/master/images/Spark_UI.png)

## Kafka Consumer Console Output
![Kafka Consumer Output](https://github.com/WinCanton/udacity-spark-kafka-sf-crime-data-streaming-analytics/blob/master/images/kafka_console_consumer_output.png)

## Stream Analytics / Count
![Stream Analytics](https://github.com/WinCanton/udacity-spark-kafka-sf-crime-data-streaming-analytics/blob/master/images/Stream_analytics.png)

## Progress Report
![Progress Report](https://github.com/WinCanton/udacity-spark-kafka-sf-crime-data-streaming-analytics/blob/master/images/Progress_Report.png)
