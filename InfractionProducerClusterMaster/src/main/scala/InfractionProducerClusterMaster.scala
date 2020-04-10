
object InfractionProducerClusterMaster extends App {

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
  import org.apache.curator.framework.api.CuratorWatcher
  import org.apache.curator.retry.ExponentialBackoffRetry
  import org.apache.zookeeper.CreateMode

  import org.apache.zookeeper.Watcher.Event.EventType
  import org.apache.zookeeper.{KeeperException, WatchedEvent}

  import java.nio.ByteBuffer
  import java.util.Arrays; 

  import scala.io.Source;
  
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


  /*initializing producers count*/
  client.setData().forPath(producerCountPath,"0".getBytes());


  /*watcher to  see InfractionProducers connections*/
  def countChangeListener(): Unit = {
      client.getData().usingWatcher(new CuratorWatcher {
              override def process(event: WatchedEvent): Unit = {
                if (event.getType != EventType.None){ // ignore connection errors
                  var bytes = client.getData().forPath(producerCountPath);
                  var count = (bytes.map(_.toChar)).mkString;
                  println("Producer count updated: "+count)
                  countChangeListener();
                }
              }
      }).forPath(producerCountPath)
  }

  countChangeListener();

  /*read line to launch csv reading*/
  val lines = Iterator.continually(scala.io.StdIn.readLine()).takeWhile(_ != "go").mkString
  println("computing file size...")
  var bytes = client.getData().forPath(producerCountPath);
  var count = (bytes.map(_.toChar)).mkString.toInt;
  //var nb = Source.fromFile("/media/william/Stormtroopr/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv").getLines.size;
  var nb = 9100279;
  println("count: "+count+", file size: "+nb);
  //compute ranges
  var nbLinesToReadPerReader = nb/count;
  var cursor = 0;
  var rangeString = "";
  rangeString = cursor+","+(cursor+nbLinesToReadPerReader);
  cursor += nbLinesToReadPerReader;
  if(count > 1){
    for( i <- 2 to count){ 
      rangeString = rangeString+" "+cursor+","+(cursor+nbLinesToReadPerReader);
      cursor += nbLinesToReadPerReader;
    }
  }

  client.setData().forPath(triggerPath,rangeString.getBytes());
  println("go! "+rangeString)
}
