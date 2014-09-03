package com.mindcandy.waterfall.service

import akka.actor.{ Actor, Props }
import spray.routing._

trait ServiceRoute extends HttpService {
  def route: Route
}

object HandlerServiceActor {
  def props(routes: Seq[ServiceRoute]): Props = Props.apply(new HandlerServiceActor(routes))
}

class HandlerServiceActor(routes: Seq[ServiceRoute]) extends Actor with HttpService {
  val aggregatedRoutes = routes.map(_.route).reduceLeft((working, directive) ⇒ working ~ directive)
  implicit val actorRefFactory = context
  def receive = runRoute(aggregatedRoutes)
}
