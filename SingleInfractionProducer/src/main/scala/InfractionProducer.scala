
object InfractionProducer extends App {

  /*Imports*/

  import java.util.Calendar;
  import java.util.Date;

  import java.util.Properties
  import org.apache.kafka.clients.producer._

  import org.json4s._
  import org.json4s.native.Serialization
  import org.json4s.native.Serialization.{read, write}


  import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
  import org.apache.curator.framework.api.CuratorWatcher
  import org.apache.curator.retry.ExponentialBackoffRetry
  import org.apache.zookeeper.CreateMode
  import org.apache.zookeeper.Watcher.Event.EventType
  import org.apache.zookeeper.{KeeperException, WatchedEvent}

  import java.nio.ByteBuffer
  import java.util.Arrays; 

  import scala.io.Source;

  import scala.util.control.Breaks._

  if (args.length == 0) {
      println("dude, i need at least one parameter")
  }else{
    var droneId = args(0);
    // json4s requires you to have this in scope to call write/read
    implicit val formats = DefaultFormats

    case class Infractions(droneId: Int,date: Date,image: String ,violationCode: Int,registrationState: String,
      plateId: String, controled: Int, vehicleColor: String, vehicleBodyType: String,streetName: String)

    //val drone1 = DroneStatus(1,"2E 93rd St New york",Calendar.getInstance().getTime())
    
    
    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all");//producer will wait for acknowledgment

    val producer = new KafkaProducer[String, String](props)
      
    val TOPIC="Infractions";

    val bufferedSource = Source.fromFile("/media/william/Stormtroopr/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv")
    var count = 0;
    for (line <- bufferedSource.getLines) {
            if(count!=0){
              val cols = line.split(",").map(_.trim)
              var infractionCode = 0;
              //2/10 chance to get unkown infraction code
              val r = scala.util.Random;
              if(r.nextInt(10)>3){
                infractionCode = cols(5).toInt;
              }
              //date conversion
              val format = new java.text.SimpleDateFormat("M/dd/yyyy")
              var date = format.parse(cols(4))

              //object fill and send
              //var jsonInfraction = write(Infractions(droneId.toInt,date,"infraction.jpg",infractionCode,cols(2),cols(1),0,cols(33),cols(6),cols(24)));
              //var jsonInfraction = write(Infractions(0,new Date(),"infraction.jpg",0,"state","plateid",0,"color","bodytype","streetname"));
              var jsonInfraction = "{'droneId':"+droneId+",'date':'"+date+"','image':'infraction.jpg','violationCode':"+infractionCode+",'registrationState':'"+cols(2)+"','plateId':'"+cols(1)+"','controled': 0,'vehicleColor': '"+cols(33)+"','vehicleBodyType':'"+cols(6)+"','streetName':'"+cols(24)+"'}";
              val record = new ProducerRecord(TOPIC, droneId+"",jsonInfraction)
              producer.send(record)
              print("record sent :"+jsonInfraction+"\n")
              //println(s"${cols(4)}|${cols(5)}|${cols(2)}|${cols(1)}|${cols(33)}|${cols(6)}|${cols(24)}")
              if(count==50)
                break;
            }
            count+=1;
    }
    bufferedSource.close
    producer.close();
  }
}
