package history

import config.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
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

    val historyList = List(HistoryStackFile("zzz", "8fa4fbf955.2019-11-04.72.ld72.json.gz", 0, "2016-01-11 00:01:02")
      ,HistoryStackFile("xxx", "8fa4fbf955.2019-11-03.72.ld72.json.gz", 0 ,"2016-01-11 00:01:02")
      ,HistoryStackFile("ccc", "ABC123456", 0, "2016-01-11 00:01:02"),HistoryStackFile("ccc", "ZXF232322323232", 0, "2016-01-11 00:01:02"))

    this.checkHistoryOfExecution("fileSourceName", historyList, spark)
  }

  def updateHistoryOfExecution(addToStackHistory: List[HistoryStackFile], spark: SparkSession): Unit = {
    import spark.implicits._

    addToStackHistory.toDF()
      .write.mode(SaveMode.Append)
      .partitionBy("appName")
      .parquet(Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()))
  }

  def checkHistoryOfExecution(columnKey: String, filesToStackProcess: List[HistoryStackFile], spark: SparkSession): List[HistoryStackFile] = {
    import spark.implicits._

    val historyInParquet = spark.read.parquet(Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()))
    val filesToAddIntoStack = filesToStackProcess.toDS().toDF()

    val result = historyInParquet.select(columnKey)
      .intersect(filesToAddIntoStack.select(columnKey))

    val itemsToRemove = result.as(Encoders.STRING).collectAsList

    def filesToProcess =
      for {
        fileName <- filesToStackProcess.filter(i => !itemsToRemove.toArray()
          .toList.contains(i.fileSourceName))
      } yield HistoryStackFile(appName = "LegacyLogDNA",
        fileSourceName = fileName.toString(),
        timestamp = DateTime.now().getMillis,
        dateTime = DateTime.now().toString)

    filesToProcess
  }
}