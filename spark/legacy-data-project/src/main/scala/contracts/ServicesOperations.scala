package contracts

trait ServicesOperations {

  def mappingByAccount(account: String): List[Map[String, Any]]
  def chargerData(anonymousID: String, typeFilter: String): String
}
