package kafka;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class Consumer {
	public static void main(String[] args) {
		//Kafka consumer configuration settings
		String topicName = "test-1";
		Properties props = new Properties();

		props.put("bootstrap.servers", "192.168.79.100:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", 
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", 
				"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer
				<String, String>(props);

		//Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		//print the topic name
		System.out.println("Subscribed to topic " + topicName);
		//int i = 0;

		/*try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records)

					// print the offset,key and value for the consumer records.
					System.out.printf("offset = %d, key = %s, value = %s\n", 
							record.offset(), record.key(), record.value());
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			System.out.println("consumer closed");
			consumer.close();
		}*/

		Thread loop = new Thread(new Runnable() {

			@Override
			public void run() {
				Thread.currentThread().setName("loop");
				try {
					while (true) {
						ConsumerRecords<String, String> records = consumer.poll(100);
						for (ConsumerRecord<String, String> record : records) {

							// print the offset,key and value for the consumer records.
							/*System.out.printf("offset = %d, key = %s, value = %s\n", 
									record.offset(), record.key(), record.value());*/

							String value = record.value();

							try (JsonReader reader = Json.createReader(new StringReader(value))) {
								JsonObject inbound = reader.readObject();
								JsonObject imas = inbound.getJsonObject("imas");
								JsonObject payload = inbound.getJsonObject("payload");
								
								MessageProducer producer = MessageProducerFactory.get(ProducerType.META);
								producer.sendMessage(imas);
								
								System.out.println("message processing completed");
								
							}catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				} catch (WakeupException e) {
					// ignore for shutdown
				} finally {
					System.out.println("consumer closed");
					consumer.close();
				}
			}
		});

		loop.start();


		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			ThreadGroup group = Thread.currentThread().getThreadGroup();
			Thread[] threads = new Thread[group.activeCount()];
			group.enumerate(threads);

			for(Thread t : threads) {
				if(t.getName().equals("loop")) {

					t.interrupt();
				}
			}
		}));
	}

	public void close() {
		System.out.println("init method called");
	}
}
