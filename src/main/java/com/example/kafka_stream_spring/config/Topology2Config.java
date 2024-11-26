package com.example.kafka_stream_spring.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class Topology2Config {

    @Bean(name = "topology2")
    public KafkaStreams kafkaStreamsTopology2() {
        // Configuración específica para la topología 2
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology2-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // Define la topología 1
        StreamsBuilder builder = new StreamsBuilder();

        builder.table(
                        "greetings",
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("WORDS")
                ).mapValues(value -> value.toUpperCase())
                .toStream()
                .to("uppercase");


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Inicia la topología
        streams.start();

        return streams;
    }
}