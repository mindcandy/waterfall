package com.mindcandy.waterfall.service

import akka.actor.ActorSystem
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.mindcandy.waterfall.actor.{ DropSupervisor, ScheduleManager, JobDatabaseManager }
import com.mindcandy.waterfall.app.{ ApplicationLifecycle, AbstractApplicationDaemon }
import com.typesafe.config.ConfigFactory
import com.mindcandy.waterfall.config.ConfigReader
import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.database
import com.mindcandy.waterfall.actor.Protocol.{ dropLogs, dropJobs }

trait ClassLoader[T] {
  def loadClass(className: String): T = {
    val factoryClass = Class.forName(className)
    factoryClass.newInstance().asInstanceOf[T]
  }

}

case class WaterfallSystem() extends ApplicationLifecycle with ConfigReader with ClassLoader[WaterfallDropFactory] {

  var isStarted = false

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("waterfall")

  def start {
    if (!isStarted) {
      isStarted = true

      val config = ConfigFactory.load()

      val db = new database.DB(logDatabase(config))
      db.createIfNotExists(List(dropJobs, dropLogs))
      val dropFactory = loadClass(dropFactoryClass(config))
      val jobDatabaseManager = system.actorOf(JobDatabaseManager.props(jobsDatabaseConfig(config), db), "job-database-manager")
      val dropSupervisor = system.actorOf(DropSupervisor.props(jobDatabaseManager, dropFactory), "drop-supervisor")
      val scheduleManager = system.actorOf(ScheduleManager.props(jobDatabaseManager, dropSupervisor, dropFactory, maxScheduleTime(config), checkJobsPeriod(config)), "schedule-manager")

      // create and start service actors
      val service = system.actorOf(JobServiceActor.props(jobDatabaseManager), "job-service")

      implicit val timeout = Timeout(5.seconds)
      // start a new HTTP server on port 8080 with our service actor as the handler
      IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
    }
  }

  def stop {
    if (isStarted) {
      IO(Http) ! Http.Unbind
      system.shutdown()
      system.awaitTermination()
    }
  }
}

case class ApplicationDaemon() extends AbstractApplicationDaemon {
  def application = WaterfallSystem()
}
