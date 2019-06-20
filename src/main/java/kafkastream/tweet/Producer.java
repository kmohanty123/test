package kafkastream.tweet;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Producer {
	private static KafkaProducer<String, UserTweetDetail> kafkaProducer=null;
	public static void main(String[] args) {
		
	 Properties props = new Properties();
   	 props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
   	 props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
     //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"kafkastream.tweet.UserSerializer");
   	final Serializer<UserTweetDetail> pageViewSerializer = new JsonPOJOSerializer<>();
    
   	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,pageViewSerializer.getClass());
     
     	UserTweetDetail tweetdetail=new UserTweetDetail();
	 	tweetdetail.setUserName("hello");
	 	tweetdetail.setMessage("I am good");
	 	tweetdetail.setGeoLocation("kernataka");
	 	Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
	 	//tweetdetail.setTime(timeStamp);
	 	
	 	UserTweetDetail tweetdetail1=new UserTweetDetail();
	 	tweetdetail1.setUserName("mohanty");
	 	tweetdetail1.setMessage("I am not good");
	 	tweetdetail1.setGeoLocation("odisha");
	 	//Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
	 	//tweetdetail.setTime(timeStamp);

     /*try  {
    	 	kafkaProducer = new KafkaProducer<>(props);
    	 	kafkaProducer.send(new ProducerRecord<String, UserTweetDetail>("testt11", tweetdetail));
    	    System.out.println("Message sent !!");
    	    try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
    	} catch (Exception e) {
    	  e.printStackTrace();
    	}*/
		
     
    org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(props);
     ObjectMapper objectMapper = new ObjectMapper();
     JsonNode  jsonNode = objectMapper.valueToTree(tweetdetail);
     String topicName="test12";
     ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
     producer.send(rec);
     try {
         Thread.sleep(1000);
     } catch (InterruptedException e1) {
         e1.printStackTrace();
     }  
    // producer.close();

	}

}
