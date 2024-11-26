package com.example.kafka_stream_spring.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TopologyController {
    private final KafkaStreams topology1;

    private final KafkaStreams topology2;

    public TopologyController(@Qualifier("topology1") KafkaStreams topology1,
                              @Qualifier("topology2") KafkaStreams topology2) {
        this.topology1 = topology1;
        this.topology2 = topology2;
    }

    @GetMapping("/topology1")
    public Object topology1(@RequestParam String key) {
        var store = topology1.store(StoreQueryParameters.fromNameAndType("WORDS", QueryableStoreTypes.keyValueStore()));
        return store.get(key);
    }

    @GetMapping("/topology2")
    public Object topology2(@RequestParam String key) {
        var store = topology2.store(StoreQueryParameters.fromNameAndType("WORDS", QueryableStoreTypes.keyValueStore()));
        return store.get(key);
    }
}
