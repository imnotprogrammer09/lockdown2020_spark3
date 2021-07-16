package sparkDataFrameTest

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object writerDF extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzSparkWrite")
      .master("local[*]")
      .getOrCreate()

    val filename = "E:\\code\\myspark3\\src\\main\\data\\flight-time.parquet"

    val flightparquet = spark.read
      .format("parquet")
      .option("path", filename)
      .load()

    flightparquet.printSchema()
    flightparquet.show(5)

    /*
    logger.info("Num partitions before: " + flightparquet.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
    flightparquet.groupBy(spark_partition_id()).count().show()

    val partitionedflight = flightparquet.repartition(5)
    logger.info("Num partitions before: " + partitionedflight.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
    partitionedflight.groupBy(spark_partition_id()).count().show()



    partitionedflight.write
        .format("avro")
        .option("path", "datasink/avro")
        .mode(SaveMode.Overwrite)
        .save()
     */

    flightparquet.write
        .format("json")
        .mode(SaveMode.Overwrite)
        .option("path", "datasink/json")
        .partitionBy("OP_CARRIER", "ORIGIN")
      .option("maxRecordsPerFile", 10000)
        .save()


    spark.close()

  }

}
