package sparkDataFrameTest

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object schemaDF extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzFlightSpark")
      .master("local[*]")
      .getOrCreate()

    val myschema = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))



    val filename = "E:\\code\\myspark3\\src\\main\\data\\flight-time.csv"

    val flightcsv = spark.read
      .format("csv")
      .option("path", filename)
      //.option("inferSchema","true")
      .schema(myschema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("dateFormat", "m/d/y")
      .load()

    flightcsv.printSchema()
    flightcsv.show(5)

    spark.close()


  }

}
