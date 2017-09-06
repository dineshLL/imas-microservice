package kafka;

public class MessageProducerFactory {
	
	public static MessageProducer get(String type) {
		
		switch (type) {
		case ProducerType.BASE:
			return BaseMessageProducer.getInstance();
		case ProducerType.META:
			return MetaMessageProducer.getInstance();

		default:
			return null;
		}
	}
	
}
