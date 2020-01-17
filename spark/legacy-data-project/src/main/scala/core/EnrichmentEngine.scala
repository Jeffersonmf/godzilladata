package core

import config.Environment
import exceptions.LoadDataException
import history.HistoryStackFile
import org.apache.spark.sql
import org.apache.spark.sql._
import org.joda.time.DateTime
import utils.Utils

object EnrichmentEngine {

  val spark = sparkContextInitialize()
  //  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
  //  this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  private def updateHistoryOfExecution(processedFiles: List[HistoryStackFile]) = {
    //TODO: Calls here the history modules.
    
  }

  def chargeSourceData(pathComplement: String): Boolean = {

    var processStatus: Boolean = false

    if (Utils.isEmptyOrNull(pathComplement))
      return false

    Environment.isRunningLocalMode() match {
      case false => processStatus = this.processInAWS(pathComplement)
      case true => processStatus = this.processInLocalStorage(pathComplement)
    }

    processStatus
  }

  private def processInAWS(path: String): Boolean = {
    this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
    this.spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

    processAndChargeData(path)
  }

  private def processInLocalStorage(path: String): Boolean = {
    processAndChargeData(path)
  }

  private def processAndChargeData(path: String): Boolean = {
    // For implicit conversions like converting RDDs to DataFrames
    var processStatus: Boolean = false

    //TODO: Colocar essa logica fora desse escopo...
    val sourcePath = Environment.getSourceFolder(Environment.isRunningLocalMode()).concat(if (path != null) path else "")
    val destPath = Environment.getParquetDestinationFolder(Environment.isRunningLocalMode())

    try {
      if (!Utils.isSourceFolderEmpty(sourcePath)) {
        val df = spark.sqlContext.read.json(sourcePath)
        df.withColumn("_source._ts", df.col("_source._ts").cast(sql.types.LongType))
        df.withColumn("_source._host", df.col("_source._host"))
        df.createOrReplaceTempView("dataFrame")

        val dftemp = spark.sql("SELECT _source._host as host, " +
          "_source._logtype as logtype, " +
          "_source._mac as mac, " +
          "_source._file as file, " +
          "_source._line as line, " +
          "_source._ts as ts, " +
          "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd HH:mm:ss.SSSS\") as datetime, " +
          "_source._app as app," +
          "_source._ip as ip," +
          "_source._ipremote as ipremote," +
          "_source.level as level," +
          "_source._lid as lid" +
          " FROM dataFrame")
        //          .withColumn("account", lit(null).cast(StringType))
        //          .toDF()
        //          .write.mode(SaveMode.Append)
        //          //.partitionBy("logtype")
        //          .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {
      if (processStatus) {

        def filesToProcess =
          for {
            file <- Utils.getListOfFiles(sourcePath).map(name => name.getName)
          } yield HistoryStackFile(appName = "LegacyLogDNA",
            fileSourceName = file,
            timestamp = DateTime.now().getMillis,
            dateTime = DateTime.now().toString)

        updateHistoryOfExecution(filesToProcess)
      }
    }

    processStatus
  }

  def deleteSourceData(complementPath: String): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getParquetDestinationFolder(Environment.isRunningLocalMode())

    try {

      val df = spark.read.format("parquet")
        .load(destPath)
        .createTempView("logdna_datalake")

      val dftemp = spark.sql("SELECT * FROM logdna_datalake where datetime not like '%2019-11%'").toDF()
        .write.mode(SaveMode.Overwrite)
        .parquet("s3a://swap-bigdata/legacy/logdna_clean.parquet")

      processStatus = true

    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {
      processStatus
    }

    processStatus
  }
}