package kafkastream.tweet;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import com.fasterxml.jackson.databind.JsonDeserializer;

public class TweetStream {

	
	public static void main(String[] args) throws InterruptedException {
		
		
		Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<UserTweetDetail> pageViewSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", UserTweetDetail.class);
        pageViewSerializer.configure(serdeProps, false);
        
        final Deserializer<UserTweetDetail> pageViewDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", UserTweetDetail.class);
        pageViewDeserializer.configure(serdeProps, false);
        
        final Serde<UserTweetDetail> pageViewByRegionSerde = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application22");
      	 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      	 //props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
      	//props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG,UserDeserializer.class);
      	props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
      	props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      	

      	 
      	 
      //	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer).getClass().getName());
      	//props.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
      	
      	final Serde<UserTweetDetail> tweetSerde = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer);
      	//final Class<? extends Serde> tweetSerde1 = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer).getClass();
      	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,JSPONSerde.class);
      	
      	final StreamsBuilder builder = new StreamsBuilder();
      	KStream<String, UserTweetDetail> views = builder.stream("test12", Consumed.with(Serdes.String(), tweetSerde));
      	views.to("test1");
      	KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Thread.sleep(5000L);
   	 /*builder.stream("test").to("test1");


   	    final Topology topology = builder.build();
   	    final KafkaStreams streams = new KafkaStreams(topology, props);
   	    final CountDownLatch latch = new CountDownLatch(1);

   	    // attach shutdown handler to catch control-c
   	    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
   	        @Override
   	        public void run() {
   	            streams.close();
   	            latch.countDown();
   	        }
   	    });

   	    try {
   	        streams.start();
   	        latch.await();
   	    } catch (Throwable e) {
   	        System.exit(1);
   	    }
   	    System.exit(0);*/
   	 
	
		/*Properties props = new Properties();
   	 props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application211");
   	 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
   	//props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG,UserDeserializer.class);
   	 props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
   	 //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserSerde.class);
   	 
   	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.);
   	props.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
   	 
   	 final StreamsBuilder builder = new StreamsBuilder();
   	 
   	 builder.stream("tes").to("test1234");

   	    final Topology topology = builder.build();
   	    final KafkaStreams streams = new KafkaStreams(topology, props);
   	    final CountDownLatch latch = new CountDownLatch(1);

   	    // attach shutdown handler to catch control-c
   	    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
   	        @Override
   	        public void run() {
   	            streams.close();
   	            latch.countDown();
   	        }
   	    });

   	    try {
   	        streams.start();
   	        latch.await();
   	    } catch (Throwable e) {
   	        System.exit(1);
   	    }
   	    System.exit(0);
   	 
		*/
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	 /*Properties props = new Properties();
   	 props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application1");
   	 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
   	 props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,Serdes.String().getClass().getName());
   	 props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
   	 props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserSerde.class.getName());
   	//props.put("default.deserialization.exception.handler", DefaultProductionExceptionHandler.class);
     
   	 StreamsBuilder builder = new StreamsBuilder();
   	 
   	 //builder.stream("kafkastream").to("kafkaoutputTopic");
   	 KStream<String, Object> source = builder.stream("kafkastream");
   	 //KTable<String, Object> sourceKtable =builder.table("kafkastream");
   	 //GlobalKTable<String, Object> sourceGtable =builder.globalTable("kafkastream");
   	
   	 source.filter((key, value) -> (((UserTweetDetail)value).getGeoLocation())!="odisha").to("test");
   	// sourceKtable.toStream().filter((key, value) -> (((UserTweetDetail)value).getGeoLocation())!="kernataka").to("test");
   	 KafkaStreams streams = new KafkaStreams(builder.build(),props);

     streams.start();

     System.out.println(streams.toString());

     Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
   	*/
   	   
   	 
   }
	
}
