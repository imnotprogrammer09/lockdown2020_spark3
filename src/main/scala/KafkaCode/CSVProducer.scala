package KafkaCode

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object CSVProducer {
  def main(args: Array[String]): Unit = {
    print("This is a simple Kafka producer to publish CSV file to Kafka topic!")

    val prop = new Properties()

    prop.put("bootstrap.servers", "192.168.217.166:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prop)
    val topicname = "titanic"
    val filename = "F:\\code\\myspark3\\data\\titanic.csv"

    for(line <- Source.fromFile(filename).getLines().drop(1)){
      val key = line.split(","){0}

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicname, key, line)

      producer.send(record)
    }

    producer.flush()
    producer.close()

  }

}
