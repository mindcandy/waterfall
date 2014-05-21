package com.mindcandy.waterfall.service

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.mindcandy.waterfall.actor.JobDatabaseManager
import com.mindcandy.waterfall.app.{ApplicationLifecycle, ApplicationRunner, AbstractApplicationDaemon}

case class WaterfallSystem() extends ApplicationLifecycle {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("waterfall")

  def start {
    // create and start our service actor
    val jobDatabase = system.actorOf(JobDatabaseManager.props)
    val service = system.actorOf(JobServiceActor.props(jobDatabase), "job-service")

    implicit val timeout = Timeout(5.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)
  }

  def stop {
    IO(Http) ! Http.Unbind
    system.shutdown()
    system.awaitTermination()
  }
}

case class ApplicationDaemon() extends AbstractApplicationDaemon {
  def application = WaterfallSystem()
}

case object WaterfallApp extends ApplicationRunner {
  def createApplication() = ApplicationDaemon()
}
