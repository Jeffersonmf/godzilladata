package routers

import colossus.core.ServerContext
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.server.RequestHandler
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler
import services.ServicesImpl

class HttpRouterHandler(context: ServerContext) extends RequestHandler(context) {

  def validate(ano: String, mes: String): Boolean = {
    val pattern = """^[0-9]+$"""

    if(ano.matches(pattern) && mes.matches(pattern)) { true } else { false }
  }

  override def handle: PartialHandler[Http] = {
    case request @ Get on Root => {
      Callback.successful(request.ok("Welcome to Scala Colossus!!!!"))
    }
    case request @ Get on Root / "monitoring" => {
      Callback.successful(request.ok("The Big Data Charger is up!!!"))
    }
    case request @ Get on Root / "charger" / "all" => {
      Callback.successful(request.ok(ServicesImpl.chargeAll()))
    }
    case request @ Get on Root / "charger" / ano / mes => {
      validate(ano,mes) match {
        case true => Callback.successful(request.ok(ServicesImpl.chargeByMonth(ano, mes)))
        case false => Callback.successful(request.ok("Path is invalid !!!"))
      }
    }
    case request @ Get on Root / "charger" / bucketName / ano / mes => {
      validate(ano,mes) match {
        case true => Callback.successful(request.ok(ServicesImpl.chargeByBucket(bucketName, (ano+"/"+mes))))
        case false => Callback.successful(request.ok("Path is invalid !!!"))
      }
    }
    case request @ Get on Root / "charger" / ano => {
      Callback.successful(request.ok(ServicesImpl.chargeByYear(ano)))
    }
    case request @ Get on Root / "delete" / ano / mes => {
      Callback.successful(request.ok(ServicesImpl.deleteDataByMonth(ano, mes)))
    }
  }
}