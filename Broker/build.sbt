name := "Lab_kafka"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
    "org.apache.kafka" %% "kafka" % "2.4.1",
    "org.json4s" %% "json4s-native" % "3.6.6",
    "org.apache.curator" % "curator-framework" % "2.6.0",
    "org.apache.curator" % "curator-recipes" % "2.6.0"
)
