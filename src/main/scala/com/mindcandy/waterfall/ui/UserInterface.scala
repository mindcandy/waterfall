package com.mindcandy.waterfall.ui

import akka.actor.ActorRefFactory
import spray.http.CacheDirectives.{`must-revalidate`, `no-cache`, `no-store`}
import spray.http.HttpHeaders.{RawHeader, `Cache-Control`}
import spray.http._
import spray.routing.{HttpService, Route}

case class UserInterface(implicit val actorRefFactory: ActorRefFactory) extends HttpService {

  private[this] val textHeader: Seq[HttpHeader] = Seq(
    `Cache-Control`(`no-cache`, `no-store`, `must-revalidate`), RawHeader("Pragma", "no-cache"), RawHeader("Expires", "0")
  )

  def route: Route = respondWithHeaders(textHeader: _*) {
    get {
      (path("") | pathPrefix("test")) {
        getFromResource("public/index.html")
      } ~
      pathPrefix("assets") {
        pathSuffixTest(".+?(css|html|js)$".r) {   _ =>
          getFromResourceDirectory("public")
        } ~
        pathSuffixTest(".+?(map)$".r) { _ =>
          respondWithMediaType(MediaTypes.`application/json`)(getFromResourceDirectory("public"))
        } ~
        pathSuffixTest(".+?(gif|png|ttf|woff)$".r) { _ ⇒
          getFromResourceDirectory("public")
        }
      }
    }
  }
}
