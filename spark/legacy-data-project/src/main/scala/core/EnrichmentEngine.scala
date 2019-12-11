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

        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

        df.toDF()
          .write
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
}