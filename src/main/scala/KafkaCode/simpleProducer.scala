package KafkaCode

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object simpleProducer {
  def main(args: Array[String]): Unit = {

    // this produce doesnot send the KEY, which means messages will be sent to topic partitions in round robin fashion.
    // ideally you will control the partition using custom partitioner ... where you mention the key of your choice.

    println("This is a simple Kafka Producer app!")

    val prop = new Properties()

    prop.put("bootstrap.servers", "192.168.217.166:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prop)
    val topicname = "simpletopic"

    for(i <- 1 to 10){
      val value1 = "hello Kafka"+i
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicname, value1)
      producer.send(record)
    }
    producer.flush()
    producer.close()

  }

}
