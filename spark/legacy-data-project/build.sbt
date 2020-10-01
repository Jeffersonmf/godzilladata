name := "Swap_BigData_Legacy"

version := "1.0"

scalaVersion := "2.11.9"

val sparkVersion = "2.4.4"

mainClass in (Compile, run) := Some("SwapEnrichLegacyConsole")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers in Global ++= Seq(
    "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
    "Maven Central Server"          at "http://repo1.maven.org/maven2",
    "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
    "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
)

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.4",
    "org.apache.spark" %% "spark-sql" % "2.4.4",
    "com.tumblr" % "colossus_2.11" % "0.9.0",
    "org.scala-lang.modules" % "scala-async_2.12" % "0.10.0",
    "net.caoticode.dirwatcher" %% "dir-watcher" % "0.1.0",
    "com.github.seratch" %% "awscala" % "0.8.+",
    "com.amazonaws" % "aws-java-sdk" % "1.11.19",
    "org.apache.hadoop" % "hadoop-aws" % "2.8.3",
    "com.amazonaws" % "aws-java-sdk-bom" % "1.11.391",
    "com.amazonaws" % "aws-java-sdk-s3"  % "1.11.621"
)

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}.jar"

