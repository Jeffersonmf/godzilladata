package contracts

trait ServicesOperations {

  def mappingByAccount(account: String): List[Map[String, Any]]
  def chargerData(anonymousID: String, typeFilter: String): String
  def chargeByMonth(ano: String, mes: String): String
  def chargeByYear(ano: String): String
  def chargeAll(): String
}
