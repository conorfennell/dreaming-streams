import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import java.util.*

fun main(args: Array<String>) {

    val bootstrapServers = System.getenv("BOOTSTRAP_SERVERS") ?: "localhost:9092"
    val applicationId = System.getenv("APPLICATION_ID") ?: "hi-kotlin"
    val topic = System.getenv("TOPIC") ?: "topic"

    val props =  Properties()

    with(props) {
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }

    val builder = StreamsBuilder()
    val stream: KStream<String, String> = builder.stream(topic)

    val mapValues = stream.mapValues { value ->
        println("VALUE: $value")
    }

    val  streams = KafkaStreams(builder.build(), props)

    streams.cleanUp()
    streams.start()

    streams.localThreadsMetadata().forEach { data -> println(data) }

    // NOTE: to shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
}