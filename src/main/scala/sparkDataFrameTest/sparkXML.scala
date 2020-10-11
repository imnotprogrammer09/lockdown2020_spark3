package sparkDataFrameTest

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

object sparkXML {
  def main(args: Array[String]): Unit = {
    print("this application will create a DF on XML file!")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzXMLSpark")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filename = "F:\\code\\myspark3\\data\\ebay.xml"
    val ebay = spark.read.option("rowTag", "listing").xml(filename)

    ebay.printSchema()
    ebay.show()

    spark.close()

  }

}
