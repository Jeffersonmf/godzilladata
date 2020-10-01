package com.contaswap.bigdata

import config.Environment
import org.apache.spark.sql.SparkSession

object SwapEnrichLegacyConsole  {

  import java.io.{File, PrintWriter}

  import scala.io.Source

  val spark = sparkContextInitialize()
  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    testSparkSubmit("main()", args)
  }

  private def testSparkSubmit(origem: String, args: Array[String]) = {

    println("Processando...")

    val writer = new PrintWriter(new File("/home/jefferson/spark-teste/Write.txt"))

    var x = ""

    if (null == args || args.length == 0) {
      x = "Nenhum valor foi enviado..."
    } else {
      x = args(0)
    }

    writer.write(s"Esse foi criado através do execução da função: ${origem} \n Arguments: " + x.toString)
    writer.close()

    Source.fromFile("/home/jefferson/spark-teste/Write.txt").foreach { x => print(x) }
  }
}
