package experiments.fintechstudio

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import contracts.SchemaContracts
import core.SwapCoreBase
import exceptions.LoadDataException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Utils

object Orders_FlashCards_ChargersAndExperiments extends SwapCoreBase {

  def QUERY_TO_CHARGER_ORDERS_FLASHCARDS = {
    "SELECT date, orderId, companyId, empresa, cnpj, flex, food, life, go, endereco, bairro, cep, cidade, estado, destinatario FROM dataFrame"
  }

  def QUERY_TO_CHARGER_ORDERS_RETURN_FLASHCARDS = {
    "SELECT _c0 as date, _c1 as orderId, _c2 as companyId, _c3 as Empresa, _c4 as account, _c5 as type, _c6 as trackcode FROM dataFrame"
  }

  def sparkSession = {
    super.getSparkContext()
  }

  override def main(args: Array[String]): Unit = {
    if (args.nonEmpty && args.size > 2) {
      val sparkSession = super.getSparkContext()

      var processname = args(0)
      var bucketname = args(1)
      var prefixFilter = args(2)

      processname match {
        case "flash-orders" => enrich_Orders_FlashCards(bucketname, prefixFilter, sparkSession)
        case "flash-orders-return-files" => enrich_Orders_FlashCards_ReturnedByFlash(bucketname, prefixFilter, sparkSession)
        case _ => argsException
      }
    } else {
      argsException
    }
  }

  def argsException = {
    throw new Exception("Argumentos inválidos para execução.  \n\n Os argumentos esperados são: arg1 = processname [flash-orders], arg2 = BucketName, arg3 = PrefixFilter [csv/flash-orders]")
  }

  private def enrich_Orders_FlashCards(bucketName: String, prefix: String, sparkSession: SparkSession): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersDestination(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)
    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region)

      if (filesToProcess.size > 0) {
        resetOrdersSourceData(sparkSession)
        processStatus = add_FieldDate_intoS3_OrdersFlash(filesToProcess, sparkSession)
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\n Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  private def add_FieldDate_intoS3_OrdersFlash(filesToProcess: Seq[String], sparkSession: SparkSession) = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersDestination(Environment.isRunningLocalMode())
    try {
      //Extract date file]
      for (name <- filesToProcess) {
        var fileName: String = name
        if (fileName.contains(".csv")) {
          fileName = fileName.replaceAllLiterally(".cvs", "")
          val dateComputaded = ("""\d+""".r findAllIn fileName).toList
          var df = sparkSession.read.option("header", "true").option("sep", ";").csv(name)
          df.withColumn("date", lit(dateComputaded.last).cast(StringType))
          df.createOrReplaceTempView("dataFrame")

          sparkSession.sql(QUERY_TO_CHARGER_ORDERS_FLASHCARDS)
            .toDF()
            .write.mode(SaveMode.Append)
            .parquet(destPath)
        }
      }
      processStatus = true
    }
    catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\n Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  private def enrich_Orders_FlashCards_ReturnedByFlash(bucketName: String, prefix: String, sparkSession: SparkSession): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersReturnFileDestination(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {
        val df = sparkSession.read.option("header", "false").csv(filesToProcess: _*)
        df.createOrReplaceTempView("dataFrame")
        sparkSession.sql(QUERY_TO_CHARGER_ORDERS_RETURN_FLASHCARDS)
          .toDF()
          .write.mode(SaveMode.Overwrite)
          .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno de Processamento no Spark.: ".concat(false.toString).concat(" \n\nDetalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  def resetOrdersSourceData(sparkSession: SparkSession): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersDestination(Environment.isRunningLocalMode())
    val sourcePath = Environment.getFlashOrdersSource(Environment.isRunningLocalMode())

    try {
      sparkSession.read.format("parquet").schema(SchemaContracts.ordersFlashSchema)
        .load(sourcePath)
        .createTempView("table_orders_flashcards")

      sparkSession.sql("select * from table_orders_flashcards where false").toDF()
        .write.mode(SaveMode.Overwrite)
        .parquet(destPath)
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {
      processStatus = true
    }

    processStatus
  }
}