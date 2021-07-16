package sparkDataFrameTest

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object sparkCSV extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    logger.info("this application will create a DF on CSV file!")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzCSVSpark")
      .master("local[*]")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("ERROR")

    val filename = "E:\\code\\myspark3\\data\\titanic.csv"

    val titanic = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(filename)

    titanic.printSchema()
    titanic.show()

    spark.close()


  }

}
