name := "Swap_BigData_Legacy"

version := "0.1"

scalaVersion := "2.11.11"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq("-deprecation", "-unchecked")
parallelExecution in Test := false
fork := true

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
    "com.tumblr" % "colossus_2.11" % "0.9.0",
    "org.scala-lang.modules" % "scala-async_2.12" % "0.10.0",
    "net.caoticode.dirwatcher" %% "dir-watcher" % "0.1.0",
    "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
    "org.apache.spark" %% "spark-core" % "2.3.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

resolvers ++= Seq(
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
    Resolver.sonatypeRepo("public")
)

pomIncludeRepository := { x => false }