package kafka;

import javax.json.JsonObject;

public interface MessageProducer {

	void sendMessage(JsonObject message);
}
