package kafkastream.tweet;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;

public class UserSerde implements Serde<UserTweetDetail> {

	private UserSerializer userSerializer=new UserSerializer();
	private UserDeserializer userDeserializer=new UserDeserializer();

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
	public UserDeserializer deserializer() {
		return userDeserializer;
	}

	@Override
	public UserSerializer serializer() {
		return userSerializer;
	}

}
