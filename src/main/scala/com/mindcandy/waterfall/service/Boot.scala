package com.mindcandy.waterfall.service

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.mindcandy.waterfall.actors.JobDatabaseActor

object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("waterfall")

  // create and start our service actor
  val jobDatabase = system.actorOf(JobDatabaseActor.props)
  val service = system.actorOf(JobServiceActor.props(jobDatabase), "job-service")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)
}