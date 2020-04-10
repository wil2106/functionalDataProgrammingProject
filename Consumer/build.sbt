name := "Lab_kafka"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
    "org.apache.kafka" %% "kafka" % "2.4.1",
    "org.json4s" %% "json4s-jackson" % "3.7.0-M2",
    "mysql" % "mysql-connector-java" % "5.1.16",
    "org.apache.commons" % "commons-email" % "1.5"
)
