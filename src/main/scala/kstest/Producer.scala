
import java.util.Properties

import org.apache.kafka.clients.producer._

object Producer {

  def main(args: Array[String]) {

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.6.133:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "input-topic"

    println("Start producer...")
    while (true) {
      producer.send(new ProducerRecord(TOPIC, "", s"kafka"))
      Thread.sleep(2000)
    }

    producer.close()
  }
}
