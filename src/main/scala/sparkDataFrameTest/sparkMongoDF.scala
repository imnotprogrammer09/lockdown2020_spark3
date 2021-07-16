package sparkDataFrameTest


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession


object sparkMongoDF {
  def main(args: Array[String]): Unit = {
    println("Starting a Mongo spark application")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("JindalzMongodbSpark")
      .master("local[*]")
      .getOrCreate()


    val conf = ConfigFactory.load



    spark.sparkContext.setLogLevel("ERROR")

    //val mongodb_database = "wb"
    //val mongodb_collection = "worldbank"

    // making a connection to MongoDB and loading data into a dataframe.
    //val mongouri = "mongodb://localhost"
    val urim = conf.getString("mongouri")

    val mongodf = spark.read
      .format("mongo")
      .option("uri", urim )
      // .option("database", mongodb_database)
      // .option("collection", mongodb_collection)
      .load()

    mongodf.printSchema()
    mongodf.show()

    spark.stop()
  }

}
