package com.mindcandy.waterfall.actor

import java.nio.file.{ Paths, Files }

import akka.testkit.{ TestProbe, TestKit }
import akka.actor.ActorSystem
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol.{ DropLog, DropJob, DropJobList }
import scala.concurrent.duration
import com.mindcandy.waterfall.config.{ DatabaseConfig, JobsDatabaseConfig }
import org.specs2.mock.Mockito
import com.github.nscala_time.time.Imports._
import scala.slick.driver.JdbcDriver.simple._
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
      insert DropLog related to unknow Drop $logToDatabaseWithUnknownKey
      send GetJobForCompletion $getJobCompletion
      send GetScheduleForCompletion $getScheduleCompletion
  """

  override def after: Any = {
    TestKit.shutdownActorSystem(system)
    Files.deleteIfExists(Paths.get("JobDatabaseManager.db"))
  }

  val config = JobsDatabaseConfig(DropJobList(List(
    DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
    DropJob(None, "ADX", "Adx", "desc", true, "0 2 * * *", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties"))
  )))

  val databaseConfig = DatabaseConfig("jdbc:sqlite:JobDatabaseManager.db")

  def getSchedule = {
    val probe = TestProbe()
    val db = mock[DB]
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    probe.send(actor, GetSchedule())

    val expectedMessage = config.dropJobList
    probe.expectMsg(duration.FiniteDuration(5, duration.SECONDS), expectedMessage) must_== expectedMessage
  }

  def logToDatabase = {
    val probe = TestProbe()
    val db = new DB(databaseConfig)
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, 1, start, end, Some("test log"), None)
    probe.send(actor, log)
    probe.expectNoMsg(duration.FiniteDuration(5, duration.SECONDS))

    val actual = db.db.withDynSession { db.dropLogs.list }
    actual must_== List(DropLog(Some(1), 1, start, end, Some("test log"), None))
  }

  def logsToDatabase = {
    val probe = TestProbe()
    val db = new DB(databaseConfig)
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, 1, start, end, Some("test log"), None)
    val log2 = DropLog(None, 2, start, None, None, Some("exception"))

    probe.send(actor, log)
    probe.send(actor, log2)

    probe.expectNoMsg(duration.FiniteDuration(5, duration.SECONDS))
    val actual2 = db.db.withDynSession { db.dropLogs.list }
    actual2 must_== List(
      DropLog(Some(1), 1, start, end, Some("test log"), None),
      DropLog(Some(2), 2, start, None, None, Some("exception")))
  }

  def logToDatabaseWithUnknownKey = {
    val probe = TestProbe()
    val db = new DB(databaseConfig)
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 0 0 0 0", TimeFrame.DAY_TODAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, -1, start, end, Some("test log"), None)
    probe.send(actor, log)
    probe.expectNoMsg(duration.FiniteDuration(5, duration.SECONDS))

    val actual = db.db.withDynSession { db.dropLogs.list }
    actual must_== List()
  }

  def getJobCompletion() = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.orElse(Some("")).toString
    val db = mock[DB]
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    probe.send(actor, GetJobForCompletion(0, testFunc))
    probe.expectMsg(config.dropJobList.jobs.lift(0).toString) must not(throwA[AssertionError])
  }

  def getScheduleCompletion() = {
    val probe = TestProbe()
    def testFunc(ls: List[DropJob]) = probe.ref ! ls.toString
    val db = mock[DB]
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    probe.send(actor, GetScheduleForCompletion(testFunc))
    probe.expectMsg(config.dropJobList.jobs.toString) must not(throwA[AssertionError])
  }
}
