package kstest

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

object Main extends App {
  println("Wow")
  val input_topic = "sentences"
  val middle_topic = "words"
  val output_topic = "gr_words"

  val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.6.133:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      p.put("zookeeper.connect", "192.168.6.133:2181")
      p.put("zookeeper.session.timeout.ms", "400")
      p.put("zookeeper.sync.time.ms", "200")
      p.put("group.id", "something")
      p.put("commit.interval.ms", "10")
      p
    }
    val builder = new StreamsBuilder()
    val WINDOW = 5 * 60 * 1000 // window size: five minutes
    val HOP = 60 * 1000 // hop: one minute

    val textLines: KStream[String, String] = builder.stream[String, String](input_topic)
    val wordCounterTime: KStream[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .windowedBy(TimeWindows.of(WINDOW).advanceBy(HOP).until(WINDOW))
      .count()
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream
      .map((wk, v) => (wk.key(), v))

    wordCounterTime.to(middle_topic)

    val wordGroups: KStream[String, String] = wordCounterTime
      .groupBy((key, _) => key)
      .reduce((s1, s2) => s1)
      .toStream
      .map((k, _) => (k.substring(0, 1), k))
      .groupBy((key, _) => key)
      .windowedBy(TimeWindows.of(WINDOW).advanceBy(HOP).until(WINDOW))
      .reduce((s1, s2) => s1 + " " + s2)
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream
      .map((wk, v) => (wk.key(), v.split(" ").toSet.mkString(" ")))

    wordGroups.to(output_topic)


    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start()


    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
    }))
}

