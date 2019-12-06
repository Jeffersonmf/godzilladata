package core

import java.io.File

import config.Environment
import exceptions.LoadDataException
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Utils

object EnrichmentEngine {

  val spark = sparkContextInitialize()

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  private def updateHistoryOfExecution(enrichmentFiles: List[File]) = {
    //TODO: Calls here the history modules.
  }

  def chargeSourceData(pathComplement: String): Boolean = {

    var processStatus: Boolean = false
    val sourcePath = Environment.getJsonSourceFolder().concat(if (pathComplement != null) pathComplement else "")
    val destPath = Environment.getParquetDestinationFolder()

    val enrichmentFiles = Utils.getListOfFiles(sourcePath)

    try {
      if (!Utils.isSourceFolderEmpty(sourcePath)) {
        val df = spark.read.format("json").json(sourcePath)

        df.toDF()
          .write
//        .partitionBy("year", "month", "day", "hour")
          .mode(SaveMode.Append)
          .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {

      if (processStatus)
        updateHistoryOfExecution(enrichmentFiles)
    }

    processStatus
  }

//  override def loadSessionByUser(anonymous_id: String): java.util.List[Sessionization] = {
//
//    import spark.implicits._
//
//    val df_AnonymousIDSearch =
//      spark.read.parquet(Environment.getParquetDestinationFolder())
//        .filter(col("anonymous_id").equalTo(anonymous_id))
//        .select(col("anonymous_id"), col("hour"),
//          col("day"),
//          col("name"),
//          col("browser_family"),
//          col("os_family"),
//          col("device_family"))
//    //.groupBy(col("day"),col("hour"), col("anonymous_id"), col("device_family"), col("browser_family"), col("os_family"))
//    //.count()
//
//    df_AnonymousIDSearch.as[Sessionization].collectAsList()
//  }
//
//  private def purgeDataProcessed(purgeList: List[File]): Unit = {
//
//    try {
//      val source = Environment.getJsonSourceFolder()
//      val destination = Environment.getPurgeDestinationFolder()
//
//      Utils.moveListOfFiles(source, destination, purgeList)
//    } catch {
//      case e: Exception => {
//        throw new PurgeDataException(e.getMessage)
//      }
//    }
//  }
//
//  override def mappingByDevices(anonymous_id: String): util.List[Map[String, Any]] = {
//
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//
//    val df =
//      spark.read.parquet(Environment.getParquetDestinationFolder())
//        .filter(col("anonymous_id").equalTo(anonymous_id))
//        .select(col("anonymous_id"), col("device_family"), col("day"), col("hour"))
//        .groupBy(col("day"), col("hour"), col("anonymous_id"), col("device_family"))
//        .count()
//
//    df.map(result => result.getValuesMap[Any](List("device_family", "count"))).collectAsList()
//  }
//
//  override def mappingByOS(anonymous_id: String): util.List[Map[String, Any]] = {
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//
//    val df =
//      spark.read.parquet(Environment.getParquetDestinationFolder())
//        .filter(col("anonymous_id").equalTo(anonymous_id))
//        .select(col("anonymous_id"), col("os_family"), col("day"), col("hour"))
//        .groupBy(col("day"), col("hour"), col("anonymous_id"), col("os_family"))
//        .count()
//
//    df.map(result => result.getValuesMap[Any](List("os_family", "count"))).collectAsList()
//  }
//
//  override def mappingByBrowser(anonymous_id: String): util.List[Map[String, Any]] = {
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//
//    val df =
//      spark.read.parquet(Environment.getParquetDestinationFolder())
//        .filter(col("anonymous_id").equalTo(anonymous_id))
//        .select(col("anonymous_id"), col("browser_family"), col("day"), col("hour"))
//        .groupBy(col("day"), col("hour"), col("anonymous_id"), col("browser_family"))
//        .count()
//
//    df.map(result => result.getValuesMap[Any](List("browser_family", "count"))).collectAsList()
//  }
}