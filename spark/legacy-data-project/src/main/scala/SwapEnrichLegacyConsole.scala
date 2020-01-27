package com.swap.datateam

object SwapEnrichLegacyConsole {

  def main(args: Array[String]): Unit = {

    import scala.io.Source
    import java.io.File
    import java.io.PrintWriter

    println("Welcome to Scala Enrichment Console Application")

    val writer = new PrintWriter(new File("Write.txt"))

    var x = ""

    if (args.length == 0) {
      x = "Nenhum valor foi enviado..."
    } else {
      x = args(0)
    }

    writer.write("Welcome to Scala Enrichment Console Application \n Arguments: " + x.toString)
    writer.close()

    Source.fromFile("Write.txt").foreach { x => print(x) }
  }
}
