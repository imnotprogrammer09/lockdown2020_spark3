package KafkaCode

import java.util.Properties
import java.util
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object simpleConsumer {
  def main(args: Array[String]): Unit = {
    println("This is a simple Kafka Consumer APP!")

    val prop = new Properties()

    prop.put("bootstrap.servers", "192.168.217.166:9092")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("group.id", "jindalzsimplecons")
    prop.put("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer[String, String](prop)
    val topicname = "simpletopic"
    consumer.subscribe(util.Collections.singletonList(topicname))

    while(true){
      val records = consumer.poll(100)
      for (record <- records.asScala){
        println(record)
      }
    }


  }

}
