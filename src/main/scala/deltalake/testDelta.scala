package deltalake

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object testDelta extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("this application will create a delta table with Flight details!")
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

    flightcsv.printSchema()
    flightcsv.show(5)

    flightcsv.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("delta-table/flight")

    logger.info("closing the app!")
    spark.close()


  }

}
