package com.fennellconor.interactive

import org.apache.kafka.streams.state.StreamsMetadata

data class HostStoreInfo(val host: String, val port: Int, val storeNames: Set<String>) {
    companion object Factory {
        fun fromStreamsMetadata(streamsMetadata: StreamsMetadata): HostStoreInfo =
                HostStoreInfo(streamsMetadata.host(), streamsMetadata.port(), streamsMetadata.stateStoreNames())
    }
}
