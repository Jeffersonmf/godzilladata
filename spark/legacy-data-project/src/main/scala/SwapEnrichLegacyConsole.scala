package com.swap.datateam

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.joda.time.DateTime

object SwapEnrichLegacyConsole extends App {

  final override def main(args: Array[String]): Unit = {
    testSparkSubmit("main()")
  }

  def run(): Unit = {
    testSparkSubmit("run()")
  }

  final def start(): String = {
    try {
      testSparkSubmit("start()")
      "Done..."
    } catch {
      case e: Exception =>
        testSparkSubmit("start() com exceção")
        "Done with Error..."
    }
  }

  final private def exec(): Unit = {
    testSparkSubmit("exec()")
  }

  private def testSparkSubmit(origem: String) = {

    import scala.io.Source
    import java.io.File
    import java.io.PrintWriter

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
