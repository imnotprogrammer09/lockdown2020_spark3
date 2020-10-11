package sparkDataFrameTest

import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SparkSession


object sparkKafkaDF {
  def main(args: Array[String]): Unit = {
    println("Starting a Kafka spark application")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzMongodbSpark")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val topicname = "titanic"
    val kafkaboot = "192.168.217.166:9092"

    val titanicdf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaboot)
      .option("subscribe", topicname)
      .load()

    titanicdf.printSchema()


  }

}
