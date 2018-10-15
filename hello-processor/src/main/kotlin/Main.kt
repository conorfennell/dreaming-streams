package com.fennellconor.interactive

import io.ktor.application.call
import io.ktor.client.HttpClient
import io.ktor.client.call.call
import io.ktor.client.engine.apache.Apache
import io.ktor.client.response.readText
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.*
import java.util.*
import java.util.concurrent.TimeUnit


object Sources {
    const val value = "value-source"
}

object Processors {
    const val value = "value-processor"
}

object Store {
    fun value(topic: String) = "value-store-$topic"
    fun recordsProcessed(topic: String) = "records-processed-$topic"
}

fun main(args: Array<String>) {
    val bootstrapServers = System.getenv("BOOTSTRAP_SERVERS") ?: ""
    val applicationId = System.getenv("APPLICATION_ID") ?: ""
    val host = System.getenv("HOST") ?: "localhost"
    val port = System.getenv("HOST_PORT")?.toInt() ?: 8080
    val topic = System.getenv("TOPIC") ?: ""

    val props =  Properties()

    StreamsConfig.APPLICATION_SERVER_CONFIG

    with(props) {
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.APPLICATION_SERVER_CONFIG, "$host:$port")
    }

    val streams = streams(props, topic)
    val metadataService = MetadataService(streams)

    val app = HTTPApp(metadataService, HostInfo(host, port), topic)

    // TODO: Find a better way to start server
    var startServer = true
    streams.setStateListener { newState, oldState ->
        println("OLD_STATE: $oldState")
        println("NEW_STATE: $newState")
        if(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING && startServer ) {
            startServer = false
            app.start()
        }
    }

    streams.start()

    streams.localThreadsMetadata().forEach { data -> println(data) }

    Runtime.getRuntime().addShutdownHook(Thread({
        println("Stopping streams")
        streams.close(10, TimeUnit.SECONDS)
        app.stop()
    }))

}

class HTTPApp(val metadataService: MetadataService,
              val hostInfo: HostInfo,
              val topic: String) {

    private val client = HttpClient(Apache)
    private val server = embeddedServer(Netty, hostInfo.port()) {
        routing {
            get("/") {
                try {
                    val recordsProcessedStore = metadataService.streams.store(Store.recordsProcessed(topic), QueryableStoreTypes.keyValueStore<String, Long>())
                    val total = recordsProcessedStore.get("total") ?: 0
                    call.respondText(total.toString(), ContentType.Text.Plain)
                } catch (ex: Exception) {
                    println(ex.message)
                }
            }
            get("/value/{key}") {
                try {
                val key = call.parameters.get("key") ?: ""
                val valueStore = metadataService.streams.store(Store.value(topic), QueryableStoreTypes.keyValueStore<String, String>())
                val metadata  = metadataService.streamsMetadataForStoreAndKey(Store.value(topic), key, StringSerializer())
                val url = "http://${metadata.host}:${metadata.port}/value/$key"

                if ("${metadata.host}:${metadata.port}" == "${hostInfo.host()}:${hostInfo.port()}") {
                    println("LOCAL $url")
                    val value = valueStore.get(key) ?: "Not Found"
                    call.respondText(value, ContentType.Text.Plain)
                } else {
                    println("REMOTE $url")
                    val value = client.call(url).response.readText()
                    call.respondText(value, ContentType.Text.Plain)
                }
            } catch (ex: Exception) {
            println(ex.message)
        }
            }
        }
    }

    fun start() {
        println("STARTING SERVER ${hostInfo.host()}:${hostInfo.port()}")
        server.start(wait = false)
        println("STARTED SERVER ${hostInfo.host()}:${hostInfo.port()}")
    }

    fun stop() {
        println("STOPPING SERVER ${hostInfo.host()}:${hostInfo.port()}")
        server.stop(10, 10, TimeUnit.SECONDS)
        println("STOPPED SERVER ${hostInfo.host()}:${hostInfo.port()}")
    }
}

fun streams(props: Properties, topic: String): KafkaStreams {

    val topology = Topology()
            .addSource(Topology.AutoOffsetReset.EARLIEST, Sources.value, topic)
            .addProcessor(Processors.value, StoreProcessorSupplier(topic), Sources.value)
            .addStateStore(storeBuilder(Store.value(topic), Serdes.String()), Processors.value)
            .addStateStore(storeBuilder(Store.recordsProcessed(topic), Serdes.Long()), Processors.value)

    return KafkaStreams(topology, props)
}

fun <T : Any> storeBuilder(name: String, serde: Serde<T>): StoreBuilder<KeyValueStore<String, T>>? {
    val storeSupplier = Stores.persistentKeyValueStore(name)
    return  Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), serde)
}

class StoreProcessor(val topic: String): AbstractProcessor<String, String>() {

    override fun process(key: String?, value: String?) {
        println("KEY: $key")
        val valueStore= context().getStateStore(Store.value(topic)) as KeyValueStore<String, String>
        val recordsProcessedStore= context().getStateStore(Store.recordsProcessed(topic)) as KeyValueStore<String, Long>

        val total = recordsProcessedStore.get("total") ?: 0
        recordsProcessedStore.put("total", total + 1)
        valueStore.put(key, value)
        println("KEYKEY: $key")

    }

}

class StoreProcessorSupplier(val topic: String): ProcessorSupplier<String, String> {
    override fun get(): Processor<String, String> {
        return StoreProcessor(topic)
    }
}


fun delete(props: Properties, consumerGroup: String) {
        val adminClient = AdminClient.create(props)

        adminClient
                .deleteConsumerGroups(mutableListOf(consumerGroup)).all()[5, TimeUnit.SECONDS]
}