package routers

import colossus.core.ServerContext
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.server.RequestHandler
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

class HttpRouterHandler(context: ServerContext) extends RequestHandler(context) {

  override def handle: PartialHandler[Http] = {
    case request @ Get on Root => {
      Callback.successful(request.ok("Welcome to Scala Colossus!!!!"))
    }
    case request @ Get on Root / "monitoring" => {
      Callback.successful(request.ok("The Big Data Charger is up!!!"))
    }
    case request @ Get on Root / "get" / "charger" / "by-all" => {
      Callback.successful(request.ok("The Charger vault is started!!!!"))
    }
  }
}