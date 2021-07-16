package sparkDataFrameTest

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import sparkDataFrameTest.sparkCSV.{getClass, logger}

object flightDF extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    //logger.info("this application will create a DF with Flight details!")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzFlightSpark")
      .master("local[*]")
      .getOrCreate()

    val filename = "E:\\code\\myspark3\\src\\main\\data\\flight-time.csv"

    val flightcsv = spark.read
        .format("csv")
      .option("path", filename)
      .option("inferSchema","true")
      .option("header", "true")
      .load()


    //flightcsv.printSchema()
    //flightcsv.show(5)
    logger.info("CSV Schema is : " + flightcsv.schema.simpleString)
    flightcsv.show(5)


    val flightjson = spark.read
        .format("json")
        .option("path","E:\\code\\myspark3\\src\\main\\data\\flight-time.json")
        .load()

    logger.info("JSON schema is : " + flightjson.schema.simpleString)

    flightjson.show(5)

    val flightparquet = spark.read
        .format("parquet")
        .option("inferschema", "true")
        .option("path","E:\\code\\myspark3\\src\\main\\data\\flight-time.parquet")
        .load()

    flightparquet.show(5)
    logger.info("Parquet Schema is : " + flightparquet.schema.simpleString)

    spark.close()



  }

}
