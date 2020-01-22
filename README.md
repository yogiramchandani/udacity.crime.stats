# Project: SF Crime Statistics with Spark Streaming

This project contains the following files as part of the submission:

* producer_server.py
* kafka_server.py
* consumer_server.py
* data_stream.py
* screenshot.zip (This file contains 3 screenshots)
  * Kafka-Consumer-Console-Output
  * Spark-Progress-Reporter
  * Spark-Streaming-UI

> How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

```python
.master("local[*]") \
```

The master property was used to set Spark locally, the wild card * runs it on all cores, this could have been a number to set the number of cores to use. This affects throughput and latency.

>What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

```python
.option("maxOffsetsPerTrigger", 200) \
```

I used maxOffsetsPerTrigger to set the batch size, setting the optimal batch size is important for throughput:

* The right batch size needs to be selected such that it is kept in pace with the rate of data ingestion from Kafka (maxRatePerPartition, described below)

```python
.option("maxRatePerPartition", 10) \
```

I used maxRatePerPartition to set rate of data ingestion from Kafka:

* The right rate along with the batch size needs to be found to maximise throughput

I was looking at the Jobs in the Spark UI to find the optimal:

![alt text](resources\Spark-Streaming-UI.jpg "Spark UI-Jobs")

## Screenshots

### Kafka-Consumer-Console-Output

![alt text](resources\Kafka-Consumer-Console-Output.jpg "Cmd line consumer")

### Spark-Progress-Reporter

![alt text](resources\Spark-Progress-Reporter.jpg "Progress Report")
![alt text](resources\Aggregation-screenshot.jpg "Aggregation Result")

### Spark-Streaming-UI

![alt text](resources\Spark-Streaming-UI.jpg "Spark UI-Jobs")
