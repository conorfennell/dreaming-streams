package com.fennellconor.interactive

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams


class MetadataService(val streams: KafkaStreams) {

    fun streamsMetadata(): List<HostStoreInfo>  {
        return streams
                .allMetadata()
                .map { streamsMetadata -> HostStoreInfo.fromStreamsMetadata(streamsMetadata) }
                .toList()
    }

    fun streamsMetdataForStore(store: String): List<HostStoreInfo> {
        return streams
                .allMetadataForStore(store)
                .map { streamsMetadata -> HostStoreInfo.fromStreamsMetadata(streamsMetadata) }
                .toList()
    }

    fun <T: Any> streamsMetadataForStoreAndKey(store: String, key: T, serializer: Serializer<T>): HostStoreInfo {
        val streamsMetadata = streams.metadataForKey(store, key, serializer)

        return HostStoreInfo.fromStreamsMetadata(streamsMetadata)
    }

}