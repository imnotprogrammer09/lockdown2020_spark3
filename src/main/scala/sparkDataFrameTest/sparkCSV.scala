package sparkDataFrameTest

import org.apache.spark.sql.SparkSession

object sparkCSV {
  def main(args: Array[String]): Unit = {
    print("this application will create a DF on CSV file!")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzCSVSpark")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filename = "F:\\code\\myspark3\\data\\titanic.csv"

    val titanic = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(filename)

    titanic.printSchema()
    titanic.show()

    spark.close()


  }

}
