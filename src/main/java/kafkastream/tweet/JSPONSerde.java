package kafkastream.tweet;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;

public class JSPONSerde implements Serde<UserTweetDetail> {

	private JsonPOJOSerializer<UserTweetDetail> userSerializer=new JsonPOJOSerializer();
	private JsonPOJODeserializer<UserTweetDetail> userDeserializer=new JsonPOJODeserializer();

	@Override
	public void close() {
		userDeserializer.close();
		userSerializer.close();
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		userSerializer.configure(configs, isKey);
		userDeserializer.configure(configs, isKey);
	 }

	@Override
	public JsonPOJODeserializer<UserTweetDetail> deserializer() {
		return userDeserializer;
	}

	@Override
	public JsonPOJOSerializer<UserTweetDetail> serializer() {
		return userSerializer;
	}
}