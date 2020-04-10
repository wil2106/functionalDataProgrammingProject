import java.sql.{PreparedStatement, SQLException}
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


import scala.collection.JavaConverters._

import java.util.Date;


import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}

object Consumer extends App {
  import org.json4s._
  import org.json4s.jackson.Serialization.read
  import java.text.SimpleDateFormat
  implicit val formats = DefaultFormats


  val props: Properties = new Properties()
  val JDBC_DRIVER = "com.mysql.jdbc.Driver"
  val DB_URL = "jdbc:mysql://remotemysql.com:3306/AwGqBPLJPA"
  val USER = "AwGqBPLJPA"
  val PASSWORD = "3ckJtC0JbL"

  import java.sql.DriverManager


  
  Class.forName(JDBC_DRIVER);

  var mySqlConnection = DriverManager.getConnection(DB_URL, USER, PASSWORD)

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "myconsumergroup")
  props.put("enable.auto.commit", "true");
  props.put("auto.commit.interval.ms", "5000");
  props.put("auto.offset.reset", "earliest");

  val statusQuery = " insert into DroneStatus (droneStatusId, date, location, droneId, image) values (?, ?, ?, ?, ?)";
  val infractionsQuery = "insert into Infractions (infractionId, droneId, date, image, violationCode, registrationState, plateId, controled, vehicleColor, vehicleBodyType, streetName) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)

  consumer.subscribe(List("DroneStatus", "Infractions").asJava)


  case class Infractions(infractionId: Int,droneId: Int,date: Date,image: String ,violationCode: Int,registrationState: String,
    plateId: String, controled: Int, vehicleColor: String, vehicleBodyType: String,streetName: String)

  case class DroneStatus(droneStatusId: Int,date: Date,location: String,droneId: Int,image: String)

  while(true) {
    val records: ConsumerRecords[String,String] = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach { record =>
      println("TOPIC", record.topic())
      if(record.topic() == "DroneStatus")
        {
          val data = read[DroneStatus](record.value())
          var droneStatus: DroneStatus = data.asInstanceOf[DroneStatus];
          var preparedStatement: PreparedStatement = mySqlConnection.prepareStatement(statusQuery);
          println("image: ", droneStatus.image);
          preparedStatement.setInt(1, droneStatus.droneStatusId)
          preparedStatement.setDate(2, new java.sql.Date(droneStatus.date.getTime()))
          preparedStatement.setString(3, droneStatus.location)
          preparedStatement.setInt(4, droneStatus.droneId)
          preparedStatement.setString(5, droneStatus.image)
          preparedStatement.addBatch()
          try {
            preparedStatement.executeBatch
            mySqlConnection.commit
            preparedStatement.clearBatch()
          } catch {
            case se: SQLException =>
              se.printStackTrace()
          }
        }
      else if(record.topic() == "Infractions")
        {
          val data = read[Infractions](record.value())
          var infraction: Infractions = data.asInstanceOf[Infractions];
          if(infraction.violationCode == 0)
          {
            val email = new SimpleEmail
            email.setHostName("smtp.gmail.com")
            email.setSmtpPort(465)
            email.setAuthenticator(new DefaultAuthenticator("julien.lariviere.01@gmail.com", "Julien1234!"))
            email.setSSLOnConnect(true)
            email.setFrom("julien.lariviere.01@gmail.com")
            email.setSubject("Alert on infraction Id: " + infraction.infractionId)
            email.setMsg("A " + infraction.vehicleColor + " car immatriculed " + infraction.plateId + " was sanctioned but violation code not found !")
            email.addTo("PrestaCops@gmail.com")
            email.send()
          }
          var preparedStatement: PreparedStatement = mySqlConnection.prepareStatement(infractionsQuery);
          preparedStatement.setInt(1, infraction.infractionId)
          preparedStatement.setInt(2, infraction.droneId)
          preparedStatement.setDate(3, new java.sql.Date(infraction.date.getTime))
          preparedStatement.setString(4, infraction.image)
          preparedStatement.setInt(5, infraction.violationCode)
          preparedStatement.setString(6, infraction.registrationState)
          preparedStatement.setString(7, infraction.plateId)
          preparedStatement.setInt(8, infraction.controled)
          preparedStatement.setString(9, infraction.vehicleColor)
          preparedStatement.setString(10, infraction.vehicleBodyType)
          preparedStatement.setString(11, infraction.streetName)
          println(preparedStatement)
          preparedStatement.addBatch()
          try {
            preparedStatement.executeBatch
            mySqlConnection.commit
            preparedStatement.clearBatch()
          } catch {
            case se: SQLException =>
              se.printStackTrace()
          }
        }
    }
  }

}