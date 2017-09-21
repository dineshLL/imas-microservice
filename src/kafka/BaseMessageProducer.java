package kafka;

import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javafx.concurrent.Task;

public class BaseMessageProducer implements MessageProducer {
	
	private static BaseMessageProducer instance = null;
	
	private Logger logger = LoggerFactory.getLogger(BaseMessageProducer.class);
	private Producer<String, String> producer = null;
	private final String topicName = "base-q";

	public static BaseMessageProducer getInstance() {
		if(instance == null) instance = new BaseMessageProducer();
		
		return instance;
	}
	
	private BaseMessageProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.100.199:9092");  
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
		logger.info("base message producer created");
	}

	@Override
	public void sendMessage(JsonObject message) {
		logger.info("starting to send the message to the base-q");
		JsonObject imas = message.getJsonObject("imas");
		
		int hop = imas.getInt("hop");
		hop++;
		String cmd = "test-2";
		String uuid = imas.getString("uuid");
		String role = "ack";

		imas = Json.createObjectBuilder()
				.add("imas", Json.createObjectBuilder()
						.add("hop", hop)
						.add("cmd", cmd)
						.add("uuid", uuid)
						.add("role", role)
						.add("containerId", ContainerIdResolver.INSTANCE.getContainerId()))
				.build();

		producer.send(new ProducerRecord<String, String>(
				topicName, 
				Integer.toString(1),
				message.toString()
				));
		
		logger.info("message sent to the base-q");
	}
	

}
