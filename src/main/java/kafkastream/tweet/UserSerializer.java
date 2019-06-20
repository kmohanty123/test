package kafkastream.tweet;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserSerializer implements Serializer<UserTweetDetail>{
	public byte[] serialize(String arg0, UserTweetDetail arg1) {
		byte[] serializedBytes = null;
		 ObjectMapper objectMapper = new ObjectMapper();
		 try {
		 serializedBytes = objectMapper.writeValueAsString(arg1).getBytes();
		 } catch (Exception e) {
		 e.printStackTrace();
		 }
		 return serializedBytes;
	}
	@Override
	public void close() {}
	
	public void configure(Map<String,?> arg0, boolean arg1) {
		
		
	}
	
}