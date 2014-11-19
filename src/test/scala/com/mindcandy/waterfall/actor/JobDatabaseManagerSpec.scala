package com.mindcandy.waterfall.actor

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.TestDatabase
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol._
import com.typesafe.config.ConfigFactory
import org.specs2.SpecificationLike
import org.specs2.mock.Mockito
import org.specs2.specification.{Grouped, Step}
import org.specs2.time.NoTimeConversions

import scala.slick.driver.JdbcDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

class JobDatabaseManagerSpec
  extends TestKit(ActorSystem("JobDatabaseManagerSpec", ConfigFactory.load()))
  with SpecificationLike
  with NoTimeConversions
  with Mockito
  with Grouped
  with TestDatabase {
  def is = s2"""
    JobDatabaseManager should

    send GetSchedule $getSchedule
    send StartAndFinishDropLog ${startAndFinishDropLog.e1}
    send StartAndFinishDropLog multiple times ${startAndFinishDropLog.e2}
    send StartAndFinishDropLog with unknown jobID $startAndFinishDropLogWithUnknownJobID
    send GetJobForCompletion $getJobCompletion
    send GetJobsForCompletion $getJobsCompletion
    send GetJobsWithDropUIDForCompletion $getJobsWithDropUIDCompletion
    send GetChildrenWithJobIDForCompletion $getChildrenForCompletion
    send GetJobForCompletion with unknown jobID $getJobCompletionWithWrongJobID
    send GetScheduleForCompletion $getScheduleCompletion
    send GetLogsForCompletion $getLogsForCompletion
    send PostJobForCompletion $postJobForCompletion

    send StartDropLog $startDropLog
    send FinishDropLog with log ${finishDropLog.e1}
    send FinishDropLog with exception ${finishDropLog.e2}
    send FinishDropLog with no message ${finishDropLog.e3}

    insert new DropJob inserts new entry into database ${insertDropJob.e1}
    insert DropJob with unknown JobID inserts new entry into database ${insertDropJob.e2}
    insert DropJob with existing JobID updates existing entry in database ${insertDropJob.e3}

    insert new DropJob with cron and parents fails ${insertDropJob.e4}
    insert new DropJob without cron or parents fails ${insertDropJob.e5}
    insert new DropJob existing parents succeeds ${insertDropJob.e6}

    insert new DropJob missing parents fails ${insertDropJob.e7}

  """ ^ Step(afterAll)

  // TODO: New to capture exception
  //  insert new DropJob missing parents fails(2) ${insertDropJob.e8}

  def afterAll = TestKit.shutdownActorSystem(system)

  def getSchedule = {
    val probe = TestProbe()
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))
    val expectedMessage = DropJobMap(
      testDropJobs.filter(_.enabled).map(job => job.jobID.get -> job).toMap
    )

    probe.send(actor, GetSchedule())
    probe.expectMsg(expectedMessage) must not(throwA[AssertionError])
  }

  def startAndFinishDropLog = new group {
    val probe = TestProbe()
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val dropLog1 = testDropLogs(0)
    probe.send(actor, StartAndFinishDropLog(dropLog1.runUID, dropLog1.jobID, dropLog1.startTime, dropLog1.endTime.get, dropLog1.logOutput, None))
    probe.expectNoMsg()

    e1 := {
      val actual = db.executeInSession(db.dropLogs.list)
      actual must_== testDropLogs.take(1)
    }

    e2 := {
      val dropLog2 = testDropLogs(4)
      probe.send(actor, StartAndFinishDropLog(dropLog2.runUID, dropLog2.jobID, dropLog2.startTime, dropLog2.endTime.get, dropLog2.logOutput, standardException))
      probe.expectNoMsg()
      db.executeInSession(db.dropLogs.list) must_== List(dropLog1, dropLog2)
    }
  }

  def startAndFinishDropLogWithUnknownJobID = {
    val probe = TestProbe()
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val dropLog = testDropLogs(0)
    probe.send(actor, StartAndFinishDropLog(dropLog.runUID, -1, dropLog.startTime, dropLog.endTime.get, dropLog.logOutput, None))
    probe.expectNoMsg()
    db.executeInSession(db.dropLogs.list) must_== List()
  }

  def startDropLog = {
    val probe = TestProbe()
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val dropLog = testDropLogs(0)
    probe.send(actor, StartDropLog(dropLog.runUID, dropLog.jobID, dropLog.startTime))
    probe.expectNoMsg()
    db.executeInSession(db.dropLogs.list) must_== List(dropLog.copy(endTime = None, logOutput = None, exception = None))
  }

  def finishDropLog = new group {
    val probe = TestProbe()
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val dropLog = testDropLogs(0)
    probe.send(actor, StartDropLog(dropLog.runUID, dropLog.jobID, dropLog.startTime))
    probe.expectNoMsg()

    e1 := {
      probe.send(actor, FinishDropLog(dropLog.runUID, dropLog.endTime.get, dropLog.logOutput, None))
      probe.expectNoMsg()
      db.executeInSession(db.dropLogs.list) must_== List(dropLog)
    }

    e2 := {
      val error = new Exception("error")
      probe.send(actor, FinishDropLog(dropLog.runUID, dropLog.endTime.get, None, Option(error)))
      probe.expectNoMsg()
      db.executeInSession(db.dropLogs.list) must_== List(dropLog.copy(logOutput = None, exception = convertException(Option(error))))
    }

    e3 := {
      probe.send(actor, FinishDropLog(dropLog.runUID, dropLog.endTime.get, None, None))
      probe.expectNoMsg()
      db.executeInSession(db.dropLogs.list) must_== List(dropLog.copy(logOutput = None))
    }
  }

  def getLogsForCompletion = {
    val probe = TestProbe()
    def testFunction(dropHistory: DropHistory) = probe.ref ! dropHistory
    val db = testDatabaseWithJobsAndLogs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetLogsForCompletion(None, None, None, None, None, None, testFunction))
    probe.expectMsgClass(classOf[DropHistory]).logs must_== testDropLogs.sortBy(x => (-x.startTime.millis, x.jobID, x.runUID.toString))
  }

  def postJobForCompletion = {
    val probe = TestProbe()
    def testFunction(dropJob: Option[DropJob]) = probe.ref ! dropJob
    val db = testDatabaseWithJobsAndLogs
    val dropJob = DropJob(None, "EXRATE", "Exchange Rate", "desc", true, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map())
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, PostJobForCompletion(dropJob, Option.empty, testFunction))
    probe.expectMsgClass(classOf[Option[DropJob]]).isDefined must beTrue
  }

  def getJobCompletion = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob.toString
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    val jobID = 2
    probe.send(actor, GetJobForCompletion(jobID, testFunc))
    val expectedDropJob = Option(testDropJobs.filter(_.jobID == Option(jobID)).head)
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobsCompletion = {
    val probe = TestProbe()
    def testFunc(dropJobList: DropJobList) =
      probe.ref ! dropJobList.toString
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobsForCompletion(testFunc))
    val expectedDropJob = DropJobList(
      testDropJobs
    )
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobsWithDropUIDCompletion = {
    val probe = TestProbe()
    def testFunc(dropJobList: DropJobList) =
      probe.ref ! dropJobList.toString
    val db = testDatabaseWithJobs
    db.insert(db.dropJobs, testDropJobs(1))
    val actor = system.actorOf(JobDatabaseManager.props(db))
    val expectedDropJob = DropJobList(List(
      testDropJobs(1),
      testDropJobs(1).copy(jobID = Some(4))
    ))

    probe.send(actor, GetJobsWithDropUIDForCompletion("EXRATE2", testFunc))
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getChildrenForCompletion = {
    val probe = TestProbe()
    def testFunc(dropJobList: DropJobList) = probe.ref ! dropJobList.toString
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))
    val expectedDropJob = DropJobList(List(testDropJobs(2)))

    probe.send(actor, GetChildrenWithJobIDForCompletion(1, testFunc))
    probe.expectMsg(expectedDropJob.toString) must not(throwA[AssertionError])
  }

  def getJobCompletionWithWrongJobID = {
    val probe = TestProbe()
    def testFunc(dropJob: Option[DropJob]) =
      probe.ref ! dropJob.toString
    val db = testDatabaseWithJobs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetJobForCompletion(300, testFunc))
    probe.expectMsg("None") must not(throwA[AssertionError])
  }

  def getScheduleCompletion = {
    val probe = TestProbe()
    def testFunc(ls: DropJobList) = probe.ref ! ls.toString
    val db = testDatabaseWithJobsAndLogs
    val actor = system.actorOf(JobDatabaseManager.props(db))

    probe.send(actor, GetScheduleForCompletion(testFunc))
    val expectedSchedule = DropJobList(List(testDropJobs(0)))
    probe.expectMsg(expectedSchedule.toString) must not(throwA[AssertionError])
  }

  def insertDropJob = new group {
    val probe = TestProbe()

    def testFunc(dropJob: Option[DropJob]) = probe.ref ! dropJob

    val db = newDB
    db.create(db.allTables)
    val actor = system.actorOf(JobDatabaseManager.props(db))

    e1 := {
      // input with no jobID
      probe.send(actor, PostJobForCompletion(testDropJobs(0).copy(jobID = None), Option.empty, testFunc))
      probe.expectMsg(Some(testDropJobs(0)))

      db.executeInSession(db.dropJobs.list) must_== testDropJobs(0) :: Nil
    }

    e2 := {
      // input with no jobID
      probe.send(actor, PostJobForCompletion(testDropJobs(0).copy(jobID = None), Option.empty, testFunc))
      probe.expectMsg(Some(testDropJobs(0)))

      // input with arbitrary jobID
      probe.send(actor, PostJobForCompletion(testDropJobs(1).copy(jobID = Some(10)), Option.empty, testFunc))
      probe.expectMsg(Some(testDropJobs(1)))

      db.executeInSession(db.dropJobs.list) must_== testDropJobs(0) :: testDropJobs(1) :: Nil
    }

    e3 := {
      // input with arbitrary jobID
      probe.send(actor, PostJobForCompletion(testDropJobs(0), Option.empty, testFunc))
      probe.expectMsg(Some(testDropJobs(0)))

      // input with existing jobID
      probe.send(actor, PostJobForCompletion(testDropJobs(0).copy(name = "test"), Option.empty, testFunc))
      val expectedDropJob = testDropJobs(0).copy(name = "test")
      probe.expectMsg(Some(expectedDropJob))
      db.executeInSession(db.dropJobs.list) must_== expectedDropJob :: Nil
    }

    e4 := {
      // input with cron and parents
      probe.send(actor, PostJobForCompletion(testDropJobs(0).copy(jobID = None), Option(List(1)), testFunc))
      probe.expectMsg(None)

      db.executeInSession(db.dropJobs.list) must_== List()
    }

    e5 := {
      // input with no cron or parents
      probe.send(actor, PostJobForCompletion(testDropJobs(0).copy(jobID = None, cron = None), None, testFunc))
      probe.expectMsg(None)

      db.executeInSession(db.dropJobs.list) must_== List()
    }

    e6 := {
      def testListFunc(dropJobList: DropJobList) = probe.ref ! dropJobList

      // input with cron
      probe.send(actor, PostJobForCompletion(testDropJobs(0), Option.empty, testFunc))
      probe.expectMsg(Some(testDropJobs(0)))

      // input with parent of above
      probe.send(actor, PostJobForCompletion(testDropJobs(2), Option(List(1)), testFunc))
      probe.expectMsg(Some(testDropJobs(2).copy(jobID = Option(2))))

      probe.send(actor, GetChildrenWithJobIDForCompletion(testDropJobs(0).jobID.get, testListFunc))
      probe.expectMsg(DropJobList(List(testDropJobs(2).copy(jobID = Option(2)))))

      db.executeInSession(db.dropJobDependencies) must_== List(DropJobDependency(1, 2))
      db.executeInSession(db.dropJobs.list) must_== List(testDropJobs(0), testDropJobs(2).copy(jobID = Option(2)))
    }

    e7 := {
      db.executeInSession(db.dropJobs.list) must_== List()
      db.executeInSession(db.dropJobDependencies) must_== List()

      // input with parent of above
      probe.send(actor, PostJobForCompletion(testDropJobs(2), Option(List(1)), testFunc))
      probe.expectMsg(None)

      db.executeInSession(db.dropJobDependencies) must_== List()
      db.executeInSession(db.dropJobs.list) must_== List()
    }

    e8 := {
      db.executeInSession(db.dropJobs.list) must_== List()
      db.executeInSession(db.dropJobDependencies) must_== List()

      // input with parent of above
      probe.send(actor, PostJobForCompletion(testDropJobs(2), Option(List(8)), testFunc))
      // TODO: How to capture exception
      probe.expectTerminated(actor)

      db.executeInSession(db.dropJobDependencies) must_== List()
      db.executeInSession(db.dropJobs.list) must_== List()
    }
  }
}