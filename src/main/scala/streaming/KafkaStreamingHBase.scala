import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object KafkaToHBaseStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaToHBaseStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_to_hbase_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("your_kafka_topic") // Replace with your Kafka topic(s)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val hbaseConf = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(hbaseConf)

        // Modify the following line to get the HBase table instance
        val table = connection.getTable(TableName.valueOf("your_hbase_table")) // Replace with your HBase table name

        partitionOfRecords.foreach { record =>
          val processedData = processRecord(record.value()) // Replace with your processing logic

          val put = new Put(Bytes.toBytes(record.key()))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes(processedData))

          table.put(put)
        }

        table.close()
        connection.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processRecord(record: String): String = {
    // Add your processing logic here and return the processed data
    s"Processed: $record"
  }
}