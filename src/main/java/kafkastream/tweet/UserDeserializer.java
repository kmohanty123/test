package kafkastream.tweet;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserDeserializer implements Deserializer<UserTweetDetail>{

	@Override
	public void close() {}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {}

	@Override
	public UserTweetDetail deserialize(String arg0, byte[] arg1) {
		ObjectMapper mapper = new ObjectMapper();
		   UserTweetDetail user = null;
		   try {
		     user = mapper.readValue(arg1, UserTweetDetail.class);
		   } catch (Exception e) {
		     e.printStackTrace();
		   }
		   return user;
	}

}
