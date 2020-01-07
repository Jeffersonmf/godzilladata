package contracts

trait ServicesOperations {

  def mappingByAccount(account: String): List[Map[String, Any]]

  def dataLakeMapping(account: String, typeFilter: String): String

  def chargeByMonth(ano: String, mes: String): String

  def chargeByYear(ano: String): String

  def chargeAll(): String

  def deleteDataByMonth(ano: String, mes: String): String
}
