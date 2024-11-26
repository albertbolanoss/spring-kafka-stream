package com.example.kafka_stream_spring.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class Topology1Config {

    @Bean(name = "topology1")
    public KafkaStreams kafkaStreamsTopology1() {
        Properties props = getProperties();

        Topology topology = getTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        return streams;
    }

    public Topology getTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        var table = builder.table(
                "uppercase",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("WORDS")
        );


        builder.stream("greetings", Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(value -> value.toUpperCase())
            .leftJoin(
                    table,
                    (greeting, word) -> word == null ? greeting : word
            ).to("greetings_spanish");


        return builder.build();
    }

    public Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology1-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return props;
    }
}