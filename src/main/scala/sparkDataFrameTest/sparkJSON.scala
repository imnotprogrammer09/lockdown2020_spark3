package sparkDataFrameTest

import org.apache.spark.sql.SparkSession

object sparkJSON {
  def main(args: Array[String]): Unit = {
    print("this application will create a DF on JSON file!")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzJSONSpark")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filename = "F:\\code\\myspark3\\data\\zips.json"

    val zips = spark.read.json(filename)

    zips.printSchema()
    zips.show()

    spark.close()



  }

}
