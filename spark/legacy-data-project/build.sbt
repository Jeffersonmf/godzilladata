name := "Swap_BigData_Legacy"

version := "0.1"

scalaVersion := "2.11.9"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.0",
    "org.apache.spark" %% "spark-sql" % "2.3.0",
    "com.tumblr" % "colossus_2.11" % "0.9.0",
    "org.scala-lang.modules" % "scala-async_2.12" % "0.10.0",
    "net.caoticode.dirwatcher" %% "dir-watcher" % "0.1.0",
    "com.github.seratch" %% "awscala" % "0.8.+",
    "com.amazonaws" % "aws-java-sdk" % "LATEST",
    "org.apache.hadoop" % "hadoop-aws" % "LATEST"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
