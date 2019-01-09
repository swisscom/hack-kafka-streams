/*
 * @ 2019 Swisscom (Schweiz) AG
 */
package com.swisscom.cloud.hack.kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HackKafkaApplication {

	private static final Logger log = LoggerFactory.getLogger(HackKafkaApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(HackKafkaApplication.class, args);
	}

	@Bean
	public StreamsBuilder streamsBuilder( //
			TimestampExtractor telegrafMetricTimestampExtractor, //
			TopicNameExtractor<String, String> customerTopicNameExtractor) {

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> logStream = builder //
				.stream("telegraf", Consumed.with(Serdes.String(), Serdes.String(), telegrafMetricTimestampExtractor, null));

		logStream.filter((k, v) -> true) //
				.to(customerTopicNameExtractor, Produced.with(Serdes.String(), Serdes.String()));

		return builder;
	}

	@Bean
	public SmartLifecycle kafkaStreams( //
			StreamsBuilder streamsBuilder, //
			Properties streamsConfig, //
			@Value("${kafka.autostart}") boolean autostart) {

		return new SmartLifecycle() {

			private boolean running;
			private KafkaStreams kafkaStreams;

			@Override
			public void start() {
				if (!this.running) {
					if (streamsBuilder != null) {
						Topology topology = streamsBuilder.build();
						log.debug(topology.describe().toString());

						kafkaStreams = new KafkaStreams(topology, streamsConfig);
						kafkaStreams.start();
						running = true;
					}
				}
			}

			@Override
			public void stop() {
				stop(null);
			}

			@Override
			public void stop(Runnable callback) {
				if (!running) {
					log.error("Kafka not running");
				} else {
					log.info("Shutting down Kafka");
					try {
						if (kafkaStreams != null) {
							final long t_out = 5L;
							if (!kafkaStreams.close(t_out, TimeUnit.SECONDS)) {
								log.warn(
										"Timed out after {} seconds trying to close Kafka stream on lifecycle stop",
										t_out);
							}
							kafkaStreams = null;
						}
					} finally {
						running = false;
					}
				}
				if (callback != null) {
					callback.run();
				}
			}

			@Override
			public boolean isRunning() {
				return running;
			}

			@Override
			public int getPhase() {
				return Integer.MAX_VALUE;
			}

			@Override
			public boolean isAutoStartup() {
				return autostart;
			}
		};
	}

	@Bean
	public TimestampExtractor telegrafMetricTimestampExtractor() {

		return new TimestampExtractor() {

			@Override
			public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

				final long timestamp = record.timestamp();
				long result;

				if (timestamp < 0) {
					String value = record.value().toString().trim();
					String[] split = value.split(" ");
					String epoch = split[split.length - 1];
					result = Long.parseLong(epoch);
				} else {
					result = timestamp;
				}

				return result;
			}
		};
	}

	@Bean
	public TopicNameExtractor<String, String> customerTopicNameExtractor() {

		return new TopicNameExtractor<String, String>() {

			@Override
			public String extract(String key, String value, RecordContext recordContext) {
				String[] split = value.split(",");
				String topic = "metric-" + split[0];
				return topic;
			}
		};
	}

	@Bean
	public Properties streamsConfig( //
			@Value("${kafka.bootstrap-servers}") String bootstrapServers, //
			@Value("${kafka.state-dir}") String statedir) {

		Properties p = new Properties();
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, "metrics-dispatcher");
		p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
		p.put(StreamsConfig.STATE_DIR_CONFIG, statedir);

		p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
		p.put("default.replication.factor", 3);

		return p;
	}

}
