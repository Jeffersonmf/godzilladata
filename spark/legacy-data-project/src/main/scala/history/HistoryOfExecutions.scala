package history

import java.sql.Timestamp
import java.time.LocalDate

import config.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

case class HistoryStackFile(appName: String, fileSourceName: String, timestamp: Long, dateTime: String)

object HistoryOfExecutions extends App {

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  override def main(args: Array[String]): Unit = {

    val spark = sparkContextInitialize()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

    val historyList = List(HistoryStackFile("aaaaa", "xxxx", 0, "2016-01-11 00:01:02")
      ,HistoryStackFile("aaaaa", "xxxx", 0 ,"2016-01-11 00:01:02")
      ,HistoryStackFile("aaaaa", "xxxx", 0, "2016-01-11 00:01:02"))

    this.checkHistoryOfExecution(historyList, spark.sparkContext, spark)
  }

  def updateHistoryOfExecution(addToStackHistory: List[HistoryStackFile], sc: SparkContext, spark: SparkSession): Unit = {

  }

  def checkHistoryOfExecution(filesToStackProcess: List[HistoryStackFile], sc: SparkContext, spark: SparkSession): List[HistoryStackFile] = {
    import spark.implicits._

    val y = filesToStackProcess.toDF()
//    val x = Seq(ProcessedStackHistory("aaaaa", "xxxx", DateTime.now()),ProcessedStackHistory("aaaaa", "xxxx", DateTime.now()),ProcessedStackHistory("aaaaa", "xxxx", DateTime.now())).toDF()

    val historyInParquet = spark.read.parquet(Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()))
    val df = filesToStackProcess.toDS().toDF()

    //List(col1, col2, col3, col4).reduce((a, b) => a intersect b)

    val list = sc.parallelize(List(("a1","b1","c1","d1"),("a2","b2","c2","d2"))).toDF()

    Nil
  }
}