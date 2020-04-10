
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

import java.util.Arrays

object Consumer extends App {
  import java.util.Properties

  

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)
    //consumer.subscribe(util.Collections.singletonList(TOPIC))
    consumer.subscribe(Arrays.asList("DroneStatus","Infraction"));
    //consumer.subscribe(Arrays.asList("DroneStatus","Infraction"), ConsumerRebalanceListener obj)// subscribe to 2 topics
    while(true){
      val records= consumer.poll(100)
      for (record<-records.asScala){
      println("Topic: " + record.topic() + 
                  ",Key: " + record.key() +  
                  ",Value: " + record.value() +
                  ", Offset: " + record.offset() + 
                  ", Partition: " + record.partition())
      }
    }
}
