package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropJobList, DropLog }
import com.mindcandy.waterfall.config.DatabaseConfig
import org.specs2.SpecificationLike
import org.specs2.mock.Mockito
import org.specs2.specification.Step
import org.specs2.time.NoTimeConversions

import scala.slick.driver.JdbcDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

class JobDatabaseManagerSpec
    extends TestKit(ActorSystem("JobDatabaseManagerSpec"))
    with SpecificationLike
    with NoTimeConversions
    with Mockito {
  def is = s2"""
    JobDatabaseManager should
      send correct schedule $getSchedule
      log to database correctly $logToDatabase
      multiple logs to database correctly $logsToDatabase
      insert DropLog related to unknow Drop $logToDatabaseWithUnknownKey
      send GetJobForCompletion $getJobCompletion
      send GetJobsForCompletion $getJobsCompletion
      send GetJobsWithDropUIDForCompletion $getJobsWithDropUIDCompletion
      send GetJobForCompletion with unknown jobID $getJobCompletionWithWrongJobID
      send GetScheduleForCompletion $getScheduleCompletion
      send GetLogsForCompletion $getLogsForCompletion
      send GetLogsForCompletion for errors $getLogsWithErrorForCompletion
      send GetLogsForCompletion for JobID $getLogsWithJobIDForCompletion
      insert new DropJob into database ${insertDropJobNew}
      insert DropJob with unknown JobID into database ${insertDropJobArbitaryJobID}
      insert DropJob with existing JobID into database ${insertDropJobExistingJobID}
  """ ^ Step(afterAll)

  def afterAll = TestKit.shutdownActorSystem(system)

  def newDatabase = new DB(DatabaseConfig(s"jdbc:h2:mem:JobDatabaseManager${UUID.randomUUID()}.db;DB_CLOSE_DELAY=-1"))

  def getSchedule = {
    val probe = TestProbe()
    val db = newDatabase
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetSchedule())

    val expectedMessage = DropJobList(
      Map(1 -> DropJob(Some(1), "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    )
    probe.expectMsg(expectedMessage) must not(throwA[AssertionError])
  }

  def logToDatabase = {
    val probe = TestProbe()
    val db = newDatabase
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, 1, start, end, Some("test log"), None)
    probe.send(actor, log)
    probe.expectNoMsg()

    val actual = db.executeInSession(db.dropLogs.list)
    actual must_== List(DropLog(Some(1), 1, start, end, Some("test log"), None))
  }

  def logsToDatabase = {
    val probe = TestProbe()
    val db = newDatabase
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, 1, start, end, Some("test log"), None)
    val log2 = DropLog(None, 2, start, None, None, Some("exception"))

    probe.send(actor, log)
    probe.expectNoMsg()
    probe.send(actor, log2)
    probe.expectNoMsg()
    db.executeInSession(db.dropLogs.list) must_== List(
      DropLog(Some(1), 1, start, end, Some("test log"), None),
      DropLog(Some(2), 2, start, None, None, Some("exception"))
    )
  }

  def logToDatabaseWithUnknownKey = {
    val probe = TestProbe()
    val db = newDatabase
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 0 0 0 0", TimeFrame.DAY_TODAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val start = DateTime.now
    val end = Some(DateTime.now + 1.hour)
    val log = DropLog(None, -1, start, end, Some("test log"), None)

    probe.send(actor, log)
    probe.expectNoMsg()
    db.executeInSession(db.dropLogs.list) must_== List()
  }

  def populateDatabase(db: DB, referenceTime: DateTime) = {
    // As the actual reference time in the method may be just a few seconds
    // later than the passed in one, it's fine in the test
    val beforeReference1 = referenceTime - 1.hour
    val beforeReference2 = referenceTime - 2.hour
    val afterReference = referenceTime + 1.hour
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, "EXRATE2", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    db.insert(
      db.dropLogs,
      List(
        DropLog(None, 1, beforeReference2, Some(afterReference), Some("log"), None),
        DropLog(None, 1, beforeReference2, Some(beforeReference1), Some("log"), None),
        DropLog(None, 1, afterReference, None, Some("log"), None),
        DropLog(None, 1, beforeReference2, None, Some("log"), None),
        DropLog(None, 1, beforeReference2, Some(afterReference), None, Some("exception")),
        DropLog(None, 1, beforeReference2, Some(beforeReference1), None, Some("exception")),
        DropLog(None, 1, afterReference, None, None, Some("exception")),
        DropLog(None, 1, beforeReference2, None, None, Some("exception")),
        DropLog(None, 2, beforeReference2, Some(afterReference), Some("log"), None),
        DropLog(None, 2, beforeReference2, Some(beforeReference1), Some("log"), None),
        DropLog(None, 2, afterReference, None, Some("log"), None),
        DropLog(None, 2, beforeReference2, None, Some("log"), None),
        DropLog(None, 2, beforeReference2, Some(afterReference), None, Some("exception")),
        DropLog(None, 2, beforeReference2, Some(beforeReference1), None, Some("exception")),
        DropLog(None, 2, afterReference, None, None, Some("exception")),
        DropLog(None, 2, beforeReference2, None, None, Some("exception"))
      )
    )
  }

  def getLogsForCompletion = {
    val probe = TestProbe()
    val db = newDatabase
    def testFunction(dropLogs: List[DropLog]) = probe.ref ! dropLogs
    populateDatabase(db, DateTime.now)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetLogsForCompletion(None, Some(1), Some(false), testFunction))
    val result = probe.expectMsgClass(classOf[List[DropLog]])
    (result.size must_== 4) and
      (result.count(_.exception.isEmpty) must_== result.size) and
      (result.count(x => x.endTime.getOrElse(x.startTime) > DateTime.now) must_== result.size)
  }

  def getLogsWithErrorForCompletion = {
    val probe = TestProbe()
    val db = newDatabase
    def testFunction(dropLogs: List[DropLog]) = probe.ref ! dropLogs
    populateDatabase(db, DateTime.now)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetLogsForCompletion(None, Some(1), Some(true), testFunction))
    val result = probe.expectMsgClass(classOf[List[DropLog]])
    (result.size must_== 4) and
      (result.count(_.logOutput.isEmpty) must_== result.size) and
      (result.count(x => x.endTime.getOrElse(x.startTime) > DateTime.now) must_== result.size)
  }

  def getLogsWithJobIDForCompletion = {
    val probe = TestProbe()
    val db = newDatabase
    def testFunction(dropLogs: List[DropLog]) = probe.ref ! dropLogs
    populateDatabase(db, DateTime.now)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetLogsForCompletion(Some(2), None, Some(false), testFunction))
    val result = probe.expectMsgClass(classOf[List[DropLog]])
    (result.size must_== 4) and
      (result.count(_.exception.isEmpty) must_== result.size) and
      (result.count(_.jobID == 2) must_== result.size)
  }

  def getJobCompletion = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.orElse(Some("")).get.toString
    val db = newDatabase
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, "EXRATE2", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobForCompletion(2, testFunc))
    val expectedDropJob = DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobsCompletion = {
    val probe = TestProbe()
    def testFunc(dropJob: List[DropJob]) =
      probe.ref ! dropJob.toString
    val db = newDatabase
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobsForCompletion(testFunc))
    val expectedDropJob = List(
      DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
      DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
    )
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobsWithDropUIDCompletion = {
    val probe = TestProbe()
    def testFunc(dropJob: List[DropJob]) =
      probe.ref ! dropJob.toString
    val db = newDatabase
    val dropUID = "EXRATE2"
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, dropUID, "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, dropUID, "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobsWithDropUIDForCompletion(dropUID, testFunc))
    val expectedDropJob = List(
      DropJob(Some(2), dropUID, "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
      DropJob(Some(3), dropUID, "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
    )
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobCompletionWithWrongJobID = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.orElse(Some("")).get.toString
    val db = newDatabase
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobForCompletion(2, testFunc))
    probe.expectMsg("") must not(throwA[AssertionError])
  }

  def getScheduleCompletion = {
    val probe = TestProbe()
    def testFunc(ls: List[DropJob]) = probe.ref ! ls.toString
    val db = newDatabase
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetScheduleForCompletion(testFunc))
    val expectedSchedule = List(
      DropJob(Some(1), "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
    )
    probe.expectMsg(expectedSchedule.toString) must not(throwA[AssertionError])
  }

  def insertDropJobNew = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob.toString
    val db = newDatabase
    db.create(db.all)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    // input with no jobID
    probe.send(actor, PostJobForCompletion(DropJob(None, "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFunc))
    val expectDropJob = DropJob(Some(1), "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map())
    probe.expectMsg(Some(expectDropJob).toString)
    db.executeInSession(db.dropJobs.list) must_== List(expectDropJob)
  }

  def insertDropJobArbitaryJobID = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob.toString
    def testFuncNoOps(dropJob: Option[DropJob]) = ()
    val db = newDatabase
    db.create(db.all)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    // input with no jobID
    probe.send(actor, PostJobForCompletion(DropJob(None, "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFuncNoOps))
    probe.expectNoMsg()
    // input with arbitary jobID
    probe.send(actor, PostJobForCompletion(DropJob(Some(3), "EXRATE2", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFunc))

    val expectDropJob = DropJob(Some(2), "EXRATE2", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map())
    probe.expectMsg(Some(expectDropJob).toString)
    db.executeInSession(db.dropJobs.list) must_== List(
      DropJob(Some(1), "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()),
      expectDropJob
    )
  }

  def insertDropJobExistingJobID = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob.toString
    def testFuncNoOps(dropJob: Option[DropJob]) = ()
    val db = newDatabase
    db.create(db.all)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    // input with no jobID
    probe.send(actor, PostJobForCompletion(DropJob(None, "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFuncNoOps))
    probe.expectNoMsg()
    // input with arbitary jobID
    probe.send(actor, PostJobForCompletion(DropJob(Some(3), "EXRATE2", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFuncNoOps))
    probe.expectNoMsg()
    // input with existing jobID
    probe.send(actor, PostJobForCompletion(DropJob(Some(2), "EXRATE3", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()), testFunc))
    val expectDropJob = DropJob(Some(2), "EXRATE3", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map())
    probe.expectMsg(Some(expectDropJob).toString)
    db.executeInSession(db.dropJobs.list) must_== List(
      DropJob(Some(1), "EXRATE1", "", "", true, "", TimeFrame.DAY_YESTERDAY, Map()),
      expectDropJob
    )
  }
}
