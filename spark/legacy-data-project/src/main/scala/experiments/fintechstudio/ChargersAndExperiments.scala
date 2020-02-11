package experiments.fintechstudio

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import core.EnrichmentEngine.spark
import core.SwapCoreBase
import exceptions.LoadDataException
import org.apache.spark.sql
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, StringType}
import org.codehaus.jackson.map.deser.std.StdDeserializer.SqlDateDeserializer
import utils.Utils

object ChargersAndExperiments extends SwapCoreBase {

  def QUERY_TO_CHARGER_ORDERS_FLASHCARDS = {"SELECT orderId, companyId, empresa, cnpj, flex, food, life, go, endereco, bairro, cep, cidade, estado, destinatario FROM dataFrame"}
  def QUERY_TO_CHARGER_ORDERS_RETURN_FLASHCARDS = {"SELECT orderId, companyId, Empresa, 'NUMERO DA CONTA' as account, 'TIPO BENEFICIO' as type, 'CÓDIGO DE RASTREIO' as trackcode, 'Data de entrega' as datedelivery FROM dataFrame"}

  override def main(args: Array[String]): Unit = {

    if (args.nonEmpty && args.size > 2) {
      val sparkContext = super.getSparkContext()
      val experimentName = getClassNameSimplified
      var processname = args(0)
      var bucketname = args(1)
      var prefixFilter = args(2)

      processname match {
        case "flash-orders" => enrich_Orders_FlashCards(bucketname, prefixFilter)
        case "flash-orders-return-files" => enrich_Orders_FlashCards_ReturnedByFlash(bucketname, prefixFilter)
        case _ => argsException
      }
    } else {
      argsException
    }
  }

  def argsException = {
    throw new Exception("Argumentos inválidos para execução.  \n\n Os argumentos esperados são: arg1 = processname [flash-orders], arg2 = BucketName, arg3 = PrefixFilter [csv/flash-orders]")
  }

  private def enrich_Orders_FlashCards(bucketName: String, prefix: String): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersDestination(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {

        val df = spark.read.option("header", "true").csv(filesToProcess: _*)
        df.createOrReplaceTempView("dataFrame")
        val dftemp = spark.sql(QUERY_TO_CHARGER_ORDERS_FLASHCARDS)
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

  private def enrich_Orders_FlashCards_ReturnedByFlash(bucketName: String, prefix: String): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersReturnFileDestination(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {
        val df = spark.read.option("header", "true").csv(filesToProcess: _*)
        df.createOrReplaceTempView("dataFrame")
        val dftemp = spark.sql(QUERY_TO_CHARGER_ORDERS_RETURN_FLASHCARDS)
//        df.withColumn("'NUMERO DA CONTA'", df.col("account").cast(sql.types.LongType))
//        df.withColumn("'TIPO BENEFICIO'", df.col("type").cast(sql.types.StringType))
//        df.withColumn("'CÓDIGO DE RASTREIO'", df.col("trackcode").cast(sql.types.StringType))
//        df.withColumn("'Data de entrega'", df.col("datetime").cast(sql.types.DateType))
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