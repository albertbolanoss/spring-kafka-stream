package com.example.kafka_stream_spring.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class Topology1ConfigTest {

	@Autowired
	private Topology1Config topology1Config;

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, String> inputTopic;
	private TestOutputTopic<String, String> outputTopic;

	private TestInputTopic<String, String> globalTableTopic;

	@BeforeEach
	public void setup() {
		Topology topology = topology1Config.getTopology();
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

		testDriver = new TopologyTestDriver(topology, props);
		inputTopic = testDriver.createInputTopic("greetings", Serdes.String().serializer(), Serdes.String().serializer());
		outputTopic = testDriver.createOutputTopic("greetings_spanish", Serdes.String().deserializer(), Serdes.String().deserializer());
		globalTableTopic = testDriver.createInputTopic("uppercase", Serdes.String().serializer(), Serdes.String().serializer());
	}

	@AfterEach
	public void tearDown() {
		testDriver.close();
	}

	@Test
	void testUppercaseTransformation() {
		inputTopic.pipeInput("key1", "hello");
		assertEquals("HELLO", outputTopic.readValue());

		inputTopic.pipeInput("key2", "world");
		assertEquals("WORLD", outputTopic.readValue());
	}



	@Test
	void testJoinWithTable() {
		globalTableTopic.pipeInput("key1", "HOLA");
		inputTopic.pipeInput("key1", "hola");
		assertEquals("HOLA", outputTopic.readValue());

		globalTableTopic.pipeInput("key2", "MUNDO");
		inputTopic.pipeInput("key2", "mundo");
		assertEquals("MUNDO", outputTopic.readValue());
	}

}