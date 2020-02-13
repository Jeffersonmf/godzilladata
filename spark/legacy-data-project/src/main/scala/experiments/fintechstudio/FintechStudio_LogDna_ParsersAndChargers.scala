package experiments.fintechstudio

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import core.SwapCoreBase
import exceptions.LoadDataException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import utils.Utils

object FintechStudio_LogDna_ParsersAndChargers extends SwapCoreBase {

  def QUERY_TO_CHARGER_ORDERS_FLASHCARDS = {"SELECT orderId, companyId, empresa, cnpj, flex, food, life, go, endereco, bairro, cep, cidade, estado, destinatario FROM dataFrame"}
  def QUERY_TO_CHARGER_ORDERS_RETURN_FLASHCARDS = {"SELECT _c0 as orderId, _c1 as companyId, _c2 as Empresa, _c3 as account, _c4 as type, _c5 as trackcode, _c6 as datedelivery FROM dataFrame"}

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
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {

        val df = sparkSession.read.option("header", "true").csv(filesToProcess: _*)
        df.createOrReplaceTempView("dataFrame")
        sparkSession.sql(QUERY_TO_CHARGER_ORDERS_FLASHCARDS)
          .withColumn("datetime", lit(null).cast(DateType))
          .toDF()
          .write.mode(SaveMode.Overwrite)
          .parquet(destPath)
        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\n Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  private def enrich_Orders_FlashCards_ReturnedByFlash(bucketName: String, prefix: String, sparkSession: SparkSession): Boolean = {
    import sparkSession.implicits._

    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersReturnFileDestination(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {
        val df = sparkSession.read.option("header", "false").csv(filesToProcess: _*)
        val rdd = df.rdd.mapPartitionsWithIndex{
          case (index, iterator) => if(index==0) iterator.drop(1) else iterator
        }
        val dfFiltered = sparkSession.sqlContext.createDataFrame(rdd, df.schema)
        dfFiltered.createOrReplaceTempView("dataFrame")
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
}