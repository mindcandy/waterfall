package com.mindcandy.waterfall.actor

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.TestDatabase
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol._
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
    with Mockito
    with TestDatabase {
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
      send PostJobForCompletion $postJobForCompletion
      insert new DropJob into database ${insertDropJobNew}
      insert DropJob with unknown JobID into database ${insertDropJobArbitaryJobID}
      insert DropJob with existing JobID into database ${insertDropJobExistingJobID}
  """ ^ Step(afterAll)

  def afterAll = TestKit.shutdownActorSystem(system)

  def getSchedule = {
    val probe = TestProbe()
    val db = newDB
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
    val db = newDB
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
    val db = newDB
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
    val db = newDB
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

  def getLogsForCompletion = {
    val probe = TestProbe()
    def testFunction(dropHistory: DropHistory) = probe.ref ! dropHistory
    val db = testDatabase
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetLogsForCompletion(None, None, None, testFunction))
    probe.expectMsgClass(classOf[DropHistory]).count must_== 16
  }

  def postJobForCompletion = {
    val probe = TestProbe()
    def testFunction(dropJob: Option[DropJob]) = probe.ref ! dropJob
    val db = testDatabase
    val dropJob = DropJob(None, "EXRATE", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, PostJobForCompletion(dropJob, testFunction))
    probe.expectMsgClass(classOf[Option[DropJob]]).isDefined must beTrue
  }

  def getJobCompletion = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.orElse(Some("")).get.toString
    val db = newDB
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
    def testFunc(dropJobList: DropJobList) =
      probe.ref ! dropJobList.toString
    val db = newDB
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
    val expectedDropJob = DropJobList(
      Map(
        1 -> DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        2 -> DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobsWithDropUIDCompletion = {
    val probe = TestProbe()
    def testFunc(dropJobList: DropJobList) =
      probe.ref ! dropJobList.toString
    val db = newDB
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
    val expectedDropJob = DropJobList(
      Map(
        2 -> DropJob(Some(2), dropUID, "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        3 -> DropJob(Some(3), dropUID, "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobCompletionWithWrongJobID = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.orElse(Some("")).get.toString
    val db = newDB
    db.create(db.all)
    db.insert(db.dropJobs, DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()))
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobForCompletion(2, testFunc))
    probe.expectMsg("") must not(throwA[AssertionError])
  }

  def getScheduleCompletion = {
    val probe = TestProbe()
    def testFunc(ls: DropJobList) = probe.ref ! ls.toString
    val db = testDatabase
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetScheduleForCompletion(testFunc))
    val expectedSchedule = DropJobList(
      Map(
        1 -> DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
    probe.expectMsg(expectedSchedule.toString) must not(throwA[AssertionError])
  }

  def insertDropJobNew = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob.toString
    val db = newDB
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
    val db = newDB
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
    val db = newDB
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
