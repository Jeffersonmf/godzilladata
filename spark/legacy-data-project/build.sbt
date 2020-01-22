name := "Swap_BigData_Legacy"

version := "0.1"

scalaVersion := "2.11.9"

mainClass in (Compile, run) := Some("SwapEnrichLegacyConsole")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.4",
    "org.apache.spark" %% "spark-sql" % "2.4.4",
    "com.tumblr" % "colossus_2.11" % "0.9.0",
    "org.scala-lang.modules" % "scala-async_2.12" % "0.10.0",
    "net.caoticode.dirwatcher" %% "dir-watcher" % "0.1.0",
    "com.github.seratch" %% "awscala" % "0.8.+",
    "com.amazonaws" % "aws-java-sdk" % "1.11.19",
    "org.apache.hadoop" % "hadoop-aws" % "2.8.5",
    "com.amazonaws" % "aws-java-sdk-bom" % "1.11.391",
    "com.amazonaws" % "aws-java-sdk-s3"  % "1.11.391"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"