package com.titicaca.data.etl

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.concurrent.CountDownLatch

fun main(args: Array<String>) {
    val builder = StreamsBuilder()
    val source = builder.stream<String, String>("streams-input")
    val sink = source.mapValues { value ->
        value.toUpperCase()
    }
    sink.to("streams-output")

    val streams = KafkaStreams(builder.build(), StreamsConfig(
            mapOf(StreamsConfig.APPLICATION_ID_CONFIG to "streams-wordcount",
                    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                    StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name
            )
    ))
    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(Thread {
        run {
            streams.close()
            latch.countDown()
        }
    })
    streams.start()
    latch.await()
}
