package sparkDataFrameTest

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object sampleObject extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    logger.info("Starting the app!")
    val conf = ConfigFactory.load()

    System.setProperty("hadoop.home.dir", conf.getString("hadoopHome"))

    val spark = SparkSession.builder()
      .appName("JindalzFlightSpark")
      .master("local[*]")
      .getOrCreate()

    logger.info("Closing the app!")
    spark.close()

  }

}
