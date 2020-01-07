package services

import contracts.ServicesOperations
import java.util

import core.EnrichmentEngine

object ServicesImpl extends ServicesOperations {

  private def parse2Json(source: util.List[Map[String, Any]], customFieldA: String, customFieldB: String): String = {

    var parsed: String = "["
    for (i <- 1 to source.size()) {

      val fieldA = source.get(i - 1).get(customFieldA).getOrElse()
      val fieldB = source.get(i - 1).get(customFieldB).getOrElse()

      parsed = parsed.concat("""{"%s":%s},""".format(fieldA, fieldB))
    }
    parsed = parsed.dropRight(1) + "]"

    return parsed
  }

  override def dataLakeMapping(account: String, typeFilter: String): String = {

    val json = ""

    //    typeFilter match {
    //      case "account" => {
    //        //json = parse2Json(mappingByAccount(account), "browser_family", "count")
    //      };
    //      case "Legacy" => {
    //        json = parse2Json(EnrichmentEngine.mappingByDevices(anonymousID), "device_family", "count")
    //      };
    //      case _ => {
    //        json = new Gson().toJson(EnrichmentEngine.loadSessionByUser(anonymousID))
    //      };
    //    }

    json
  }

  override def mappingByAccount(account: String): List[Map[String, Any]] = {
    Nil
  }

  override def chargeByYear(ano: String): String = {""}

  override def chargeByMonth(ano: String, mes: String): String = {
    val complementPath = ano.concat("/").concat(mes).concat("/")

    EnrichmentEngine.chargeSourceData(complementPath) match {
      case true => "{Processed: \"OK\"}"
      case false => "{Processed: \"ERROR\"}"
    }
  }

  override def chargeAll(): String = {""}

  override def deleteDataByMonth(ano: String, mes: String): String = {
    val complementPath = ano.concat("/").concat(mes).concat("/")

    EnrichmentEngine.deleteSourceData(complementPath) match {
      case true => "{Processed: \"OK\"}"
      case false => "{Processed: \"ERROR\"}"
    }
  }
}