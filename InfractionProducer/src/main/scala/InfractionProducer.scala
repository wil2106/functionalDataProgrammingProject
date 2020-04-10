
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

  
  var myProducerID = -1

  
  /* Zookeeper connection */
  val ZOOKEEPER_PORT = 2181
  implicit var client: CuratorFramework = null
  client = CuratorFrameworkFactory.builder()
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("drone")
      .connectString(s"127.0.0.1:$ZOOKEEPER_PORT").build()
  client.start()

  
  /*create producer count node if not existing*/
  var producerCountPath = "/producerCount";
  var stat = client.checkExists().forPath(producerCountPath);
  if (stat == null) {
      client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(producerCountPath);
  }

  /*create trigger node if not existing*/
  var triggerPath = "/trigger";
  stat = client.checkExists().forPath(triggerPath);
  if (stat == null) {
      client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(triggerPath);
  }


  /*read and update producer count*/
  var bytes = client.getData().forPath(producerCountPath);
  var string = (bytes.map(_.toChar)).mkString;
  var count = 1;
  if(string.length>0){
    count = string.toInt;
    count = count + 1; 
  }
  println("current count: "+count);
  var newCountString = count+"";
  //update zookeeper
  client.setData().forPath(producerCountPath,newCountString.getBytes());
  //set id
  myProducerID = count;


  /*shut down hook to decrease count*/
  sys.addShutdownHook({
    var bytes = client.getData().forPath(producerCountPath);
    var string = (bytes.map(_.toChar)).mkString;
    var count = string.toInt;
    if(count>0){
      count= count - 1;
      println("closing app, current count: "+count);
      var newCountString = count+"";
      client.setData().forPath(producerCountPath,newCountString.getBytes());
    }
  });


  /*watcher for trigger*/
  def triggerChangeListener(): Unit = {
      client.getData().usingWatcher(new CuratorWatcher {
              override def process(event: WatchedEvent): Unit = {
                if (event.getType != EventType.None){ // ignore connection errors
                  var bytes = client.getData().forPath(triggerPath);
                  var triggerString = (bytes.map(_.toChar)).mkString;
                  var rangeArray = triggerString.split(" ");
                  var myRange = rangeArray(myProducerID-1).split(",");
                  println("Reading csv from "+myRange(0)+" to "+myRange(1))
                }
              }
      }).forPath(triggerPath)
  }

  triggerChangeListener();


  /*read line to prevent app from closing when finished*/
  scala.io.StdIn.readLine()






  /*
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

  /*
  // json4s requires you to have this in scope to call write/read
  implicit val formats = DefaultFormats

  case class DroneStatus(id:Int,location:String,time:Date)

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
      var jsonStatus = write(DroneStatus(myId,"2E 93rd St New york",Calendar.getInstance().getTime()));
      val record = new ProducerRecord(TOPIC, myId+"",jsonStatus)
      producer.send(record)
      print("record sent :"+jsonStatus+"\n")
    } 
  }
  timer.schedule(task, 0, 10000)
  */
  */
}
