
object DroneStatusProducer extends App {

  /*Imports*/

  import java.util.Calendar;
  import java.util.Date;

  import java.util.Properties
  import org.apache.kafka.clients.producer._

  import org.json4s._
  import org.json4s.native.Serialization
  import org.json4s.native.Serialization.{read, write}

  import java.util.Timer;


  import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
  import org.apache.curator.retry.ExponentialBackoffRetry
  import org.apache.zookeeper.CreateMode

  import java.nio.ByteBuffer
  import java.util.Arrays; 

  
  /* Zookeeper connection */
  val ZOOKEEPER_PORT = 2181
  implicit var client: CuratorFramework = null
  client = CuratorFrameworkFactory.builder()
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("drone")
      .connectString(s"127.0.0.1:$ZOOKEEPER_PORT").build()
  client.start()


  /* create /ids node if not existing*/
  var path = "/ids";
  var stat = client.checkExists().forPath(path);

  if (stat == null) {
      client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/ids");
  }

   /* get last drone id from zookeeper*/

  var bytes = client.getData().forPath(path);
  var idString = (bytes.map(_.toChar)).mkString;
  
  var myId = 1; 
  if(idString.length>0){
    var lastId = idString.split(",").last.toInt;
    myId = lastId + 1;
  }

  println("My drone id is "+myId);


  /* add my drone id to the String in zookeeper*/
    var newIdString = idString+myId+",";
  client.setData().forPath(path,newIdString.getBytes());


  client.close();



  // json4s requires you to have this in scope to call write/read
  implicit val formats = DefaultFormats

  case class DroneStatus(date: Date,location: String,droneId: Int,image: String)

  //val drone1 = DroneStatus(1,"2E 93rd St New york",Calendar.getInstance().getTime())
  
  
  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "0");//producer wont wait for acknowledgment but throughput will be bigger

  val producer = new KafkaProducer[String, String](props)
    
  val TOPIC="DroneStatus"

  //
  val timer = new java.util.Timer()
  val task = new java.util.TimerTask {
    def run(){
      var jsonStatus = write(DroneStatus(Calendar.getInstance().getTime(),"2E 93rd St New york",myId,"street.jpg"));
      val record = new ProducerRecord(TOPIC, myId+"",jsonStatus)
      producer.send(record)
      print("record sent :"+jsonStatus+"\n")
    } 
  }
  timer.schedule(task, 0, 10000)

  scala.io.StdIn.readLine()
  timer.cancel();
  timer.purge();
  producer.close()
  println("## Producer closed ##")
}
