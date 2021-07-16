package sparkDataFrameTest

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkDB extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //System.setProperty("spark.sql.warehouse.dir", "F:\\code\\myspark3\\sparkwarehouse")
    //System.setProperty("hive.metastore.warehouse.dir", "F:\\code\\myspark3\\hivewarehouse")

    val spark = SparkSession.builder()
      .appName("JindalzSparkDB")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", "F:\\code\\myspark3\\sparkwarehouse")
      //.config("hive.metastore.warehouse.dir", "F:\\code\\myspark3\\hivewarehouse")
      .enableHiveSupport()
      .getOrCreate()

    val filename = "E:\\code\\myspark3\\src\\main\\data\\flight-time.parquet"

    val flightparquet = spark.read
      .format("parquet")
      .option("path", filename)
      .load()

    import spark.sql
    sql("create database if not exists airlinedb")
    sql("use airlinedb")

    flightparquet.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("flighttab")
    spark.catalog.listTables("airlinedb").show()
    logger.info("Finished.")
    spark.stop()


  }

}
