package com.mindcandy.waterfall.actor

import java.nio.file.{Paths, Files}

import akka.testkit.{ TestProbe, TestKit }
import akka.actor.ActorSystem
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.{DropLog, DropJob, DropJobList}
import scala.concurrent.duration
import com.mindcandy.waterfall.config.JobsDatabaseConfig
import com.mindcandy.waterfall.database
import org.specs2.mock.Mockito
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.Protocol.{ dropJobs, dropLogs }
import scala.slick.driver.SQLiteDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

class JobDatabaseManagerSpec
  extends TestKit(ActorSystem("JobDatabaseManagerSpec"))
  with SpecificationLike
  with After
  with NoTimeConversions
  with Mockito {
  override def is = sequential ^ s2"""
    JobDatabaseManager should
      send correct schedule $getSchedule
      log to database correctly $logToDatabase
      multiple logs to database correctly $logsToDatabase
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  val config = JobsDatabaseConfig(DropJobList(List(
    DropJob(None, "EXRATE", "Exchange Rate", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
    DropJob(None, "ADX", "Adx", true, "0 2 * * *", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties"))
  )))

  def getSchedule = {
    val probe = TestProbe()
    val db = mock[database.DB]
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    probe.send(actor, GetSchedule())

    val expectedMessage = config.dropJobList
    probe.expectMsg(duration.FiniteDuration(5, duration.SECONDS), expectedMessage) must_== expectedMessage
  }

  def logToDatabase = {
    val probe = TestProbe()
    val db = new database.DB("jdbc:sqlite:JobDatabaseManager.db")
    db.create(List(dropJobs, dropLogs))
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(Some(1), "EXRATE", start, end, Some("test log"), None)
    probe.send(actor, log)
    probe.expectNoMsg()

    val actual = db.db.withDynSession {dropLogs.list}
    Files.deleteIfExists(Paths.get("JobDatabaseManager.db"))
    actual must_== List(log)
  }

  def logsToDatabase = {
    val probe = TestProbe()
    val db = new database.DB("jdbc:sqlite:JobDatabaseManager.db")
    db.create(List(dropJobs, dropLogs))
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(Some(1), "EXRATE", start, end, Some("test log"), None)
    val log2 = DropLog(Some(1), "ADX", start, None, None, Some("exception"))

    probe.send(actor, log)
    probe.send(actor, log2)

    probe.expectNoMsg()
    val actual2 = db.db.withDynSession {dropLogs.list}
    Files.deleteIfExists(Paths.get("JobDatabaseManager.db"))
    actual2 must_== List(log, log2)
  }
}
