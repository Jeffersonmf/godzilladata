package core

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import config.Environment
import exceptions.LoadDataException
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.File
import utils.Utils

object EnrichmentEngine {

  val spark = sparkContextInitialize()
  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

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

    if (Utils.isEmptyOrNull(pathComplement))
      return false

      Environment.isRunningAWSMode() match {
        case false => processStatus = this.processInLocalStorage(pathComplement)
        case true => processStatus = this.processInAWS(pathComplement)
      }

    processStatus
  }

  private def processInAWS(path: String): Boolean = {

    var processStatus: Boolean = false

    val sourcePath = Environment.get_AWS_JsonSourceFolder().concat(if (path != null) path else "")
    val destPath = Environment.get_AWS_ParquetDestinationFolder()

    val enrichmentFiles = Utils.getListOfFiles(sourcePath)

      try {
      if (!Utils.isSourceFolderEmpty(sourcePath)) {
        val df = spark.sqlContext.read.json(sourcePath)
        df.registerTempTable("dataFrame")

        spark.sql("SELECT _source._host as host, " +
          "_source._logtype as logtype, " +
          "_source._mac as mac, " +
          "_source._file as file, " +
          "_source._line as line, " +
          "_source._ts as ts, " +
          "_source._app as app," +
          "_source._ip as ip," +
          "_source._ipremote as ipremote," +
          "_source.level as level," +
          "_source._lid as lid," +
          "_source._tag[0] as tag FROM dataFrame").toDF()
          .write.mode(SaveMode.Append).parquet(Environment.get_AWS_ParquetDestinationFolder())

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

  private def processInLocalStorage(path: String): Boolean = {
    var processStatus: Boolean = false

    val sourcePath = Environment.getJsonSourceFolder().concat(if (path != null) path else "")
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