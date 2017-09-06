package kafka;

import java.util.Date;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaMessageProducer implements MessageProducer {

	private static MetaMessageProducer instance = null;

	private Logger logger = LoggerFactory.getLogger(MetaMessageProducer.class);
	private Producer<String, String> producer = null;
	private final String topicName = "base-q";


	@Override
	public void sendMessage(JsonObject imas) {
		System.out.println("meta message sent started " + new Date().toString());
		/*Thread thread = new Thread(new Task<Void>() {

			@Override
			protected Void call() throws Exception {*/
				try {
					int hop = imas.getInt("hop");
					hop++;
					String cmd = "ack";
					String uuid = imas.getString("uuid");
					String role = "ack";

					JsonObject json = Json.createObjectBuilder()
							.add("imas", Json.createObjectBuilder()
									.add("hop", hop)
									.add("cmd", cmd)
									.add("uuid", uuid)
									.add("role", role))
							.build();


					producer.send(new ProducerRecord<String, String>(
							topicName, 
							Integer.toString(1),
							json.toString()
							));
					System.out.println("sending message to " + topicName + " json message " + json.toString());
				}catch (Exception e) {
					e.printStackTrace();
					// TODO: handle exception
				}

				System.out.println("meta data sent to the server");

				/*return null;
			}
		});

		thread.setDaemon(true);
		thread.start();*/
	}

	public static MessageProducer getInstance() {
		if(instance == null) instance = new MetaMessageProducer();

		return instance;
	}

	private MetaMessageProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.79.100:9092");  
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);   
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer
				<String, String>(props);
		logger.info("meta message producer created");
	}

}
