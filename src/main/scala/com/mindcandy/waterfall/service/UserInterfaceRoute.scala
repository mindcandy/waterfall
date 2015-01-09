package com.mindcandy.waterfall.service

import akka.actor.ActorRefFactory
import spray.http.CacheDirectives.{ `must-revalidate`, `no-cache`, `no-store` }
import spray.http.HttpHeaders.{ RawHeader, `Cache-Control` }
import spray.http._
import spray.routing.Route

case class UserInterfaceRoute(implicit val actorRefFactory: ActorRefFactory) extends ServiceRoute {

  private[this] val textHeader: Seq[HttpHeader] = Seq(
    `Cache-Control`(`no-cache`, `no-store`, `must-revalidate`), RawHeader("Pragma", "no-cache"), RawHeader("Expires", "0")
  )

  def route: Route = respondWithHeaders(textHeader: _*) {
    // format: OFF
    get {
      (path("") | pathPrefix("ui")) {
        getFromResource("public/index.html")
      } ~
      pathPrefix("") {
        pathSuffixTest(".+?(css|html|js|gif|png|ttf|woff)$".r) { _ =>
          getFromResourceDirectory("public")
        } ~
        pathSuffixTest(".+?(map)$".r) { _ =>
          respondWithMediaType(MediaTypes.`application/json`)(getFromResourceDirectory("public"))
        }
      }
    }
    // format: ON
  }
}
