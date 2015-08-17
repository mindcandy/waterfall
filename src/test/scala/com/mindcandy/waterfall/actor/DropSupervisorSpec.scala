package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.DropSupervisor.{ RunJobImmediately, JobResult, StartJob }
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol.DropJob
import com.mindcandy.waterfall.{ TestPassThroughWaterfallDrop, TestWaterfallDropFactory }
import com.typesafe.config.ConfigFactory
import org.joda.time.chrono.ISOChronology
import org.specs2.SpecificationLike
import org.specs2.specification.Step
import org.specs2.time.NoTimeConversions

import scala.util.{ Failure, Success }

class DropSupervisorSpec extends TestKit(ActorSystem("DropSupervisorSpec", ConfigFactory.load()))
    with SpecificationLike
    with NoTimeConversions {
  def is = s2"""
    DropSupervisor should

      run the job when it receives a start job message $runJobOnStartJob
      log the job when it receives a start job message $logJobOnStartJob
      log a success result when a job is completed successfully $logSuccess
      log a failure result when a job is completed unsuccessfully $logFailure
      do not run a job that is still running $doNotStartIfRunning
      run job with identical jobID if parallel allowed $runJobsWithIdenticalJobIDIfParallelAllowed
      rerun a job if previous run finished $reRunAfterFinished
      log to database if the drop not in factory $logToErrorIfNoFactoryDrop
      log to database if result not in running list $logToErrorIfResultNotInList
      start the job for current date when it receives a start job message with no date $startJobForDefaultDateOnStartJob
      start the job for specified date when it receives a start job message with date $startJobForSpecifiedDropDateOnStartJob
      start the job for specified date and timeframe when it receives a start job message with date and timeframe is not current $startJobForSpecifiedDropDateAndTimeframeOnStartJob
      run the job for current date when it receives a run job immediately message with no date $runJobImmediatelyWithDefaultDate
      run the job for specified date when it receives a run job immediately message with a date $runJobImmediatelyWithDefaultDate

  """ ^ Step(afterAll)

  def afterAll: Any = TestKit.shutdownActorSystem(system)

  def runJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob()

    probe.send(actor, request)
    val drop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).waterfallDrop
    drop must_== TestPassThroughWaterfallDrop(Some(LocalDate.now.toDateTimeAtStartOfDay))
  }

  def logJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory))
    val request = createStartJob()

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).jobID must_== 1
  }

  def logSuccess = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_THREE_DAYS_AGO)

    probe.send(actor, request)
    val runUID = jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).runUID

    val result = JobResult(runUID, LocalDate.now, Success(()))
    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, None) => success
      case _ => failure
    }
    jobDatabaseManager.expectMsgClass(classOf[GetChildrenWithJobIDForCompletion]).parentJobId must_== 1
  }

  def logFailure = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_TWO_DAYS_AGO)
    val exception = new RuntimeException("test exception")

    probe.send(actor, request)
    val runUID = jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).runUID

    val result = JobResult(runUID, LocalDate.now, Failure(exception))
    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, Some(msg)) => success
      case _ => failure
    }
  }

  def doNotStartIfRunning = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_YESTERDAY)

    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])
    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(classOf[StartAndFinishDropLog]) match {
      case StartAndFinishDropLog(runUID, 1, _, _, None, Some(exception)) => {
        val expectedMsg = s"job 1 with drop uid test1 and name Exchange Rate has already been running, run $runUID cancelled"
        exception.getMessage match {
          case `expectedMsg` => success
          case _ => failure
        }
      }
      case s: StartAndFinishDropLog => failure
    }
  }

  def runJobsWithIdenticalJobIDIfParallelAllowed = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val startJob = createStartJob(TimeFrame.DAY_YESTERDAY)
    val request = startJob.copy(job = startJob.job.copy(parallel = true))

    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])
    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog]) must not(throwA[AssertionError])
  }

  def reRunAfterFinished = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_TODAY)

    probe.send(actor, request)
    val runUID = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).runUID

    probe.send(actor, JobResult(runUID, LocalDate.now, Success(Unit)))
    probe.send(actor, request)

    val newRunUID = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).runUID
    runUID must_!= newRunUID
  }

  def logToErrorIfNoFactoryDrop = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val dropUID = "drop not in factory"
    val request = StartJob(
      1,
      DropJob(Some(1), dropUID, "", "", true, Option(""), TimeFrame.DAY_TODAY, Map(), false, Option.empty))

    probe.send(actor, request)
    val expectedMsg = s"factory has no drop for job ${request.jobID} with drop uid ${request.job.dropUID} and name ${request.job.name}"
    jobDatabaseManager.expectMsgClass(classOf[StartAndFinishDropLog]) match {
      case StartAndFinishDropLog(_, 1, _, _, None, Some(exception)) => exception.getMessage match {
        case `expectedMsg` => success
        case _ => failure
      }
      case s: StartAndFinishDropLog => failure
    }
  }

  def logToErrorIfResultNotInList = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))

    probe.send(actor, createStartJob(TimeFrame.DAY_TODAY))
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])

    val runUID = UUID.randomUUID()
    probe.send(actor, JobResult(runUID, LocalDate.now, Success(())))

    val expectedMsg = s"job result from runUID ${runUID} but not present in running jobs list"
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, Some(exception)) => exception.getMessage match {
        case `expectedMsg` => success
        case _ => failure
      }
      case _ => failure
    }
  }

  def startJobForDefaultDateOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(runDate = None)

    probe.send(actor, request)
    val drop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).waterfallDrop
    drop must_== TestPassThroughWaterfallDrop(Some(LocalDate.now.toDateTimeAtStartOfDay))
  }

  def startJobForSpecifiedDropDateOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(runDate = Some(LocalDate.lastWeek))

    probe.send(actor, request)
    val drop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).waterfallDrop
    drop must_== TestPassThroughWaterfallDrop(Some(LocalDate.lastWeek.toDateTimeAtStartOfDay))
  }

  def startJobForSpecifiedDropDateAndTimeframeOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(frame = TimeFrame.DAY_THREE_DAYS_AGO, runDate = Some(LocalDate.lastWeek))

    probe.send(actor, request)
    val drop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).waterfallDrop
    drop must_== TestPassThroughWaterfallDrop(Some(LocalDate.lastWeek.minusDays(3).toDateTimeAtStartOfDay))
  }

  def runJobImmediatelyWithDefaultDate = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))

    var run: Boolean = false
    val completionFunc = (job: Option[DropJob]) => {
      run = true
    }

    val request = RunJobImmediately(1, None, completionFunc)
    probe.send(actor, request)
    val getJobForCompletion = jobDatabaseManager.expectMsgClass(classOf[GetJobForCompletion])

    val now = DateTime.now(ISOChronology.getInstanceUTC) + Period.seconds(3)
    getJobForCompletion.completionFunction(Some(DropJob(Some(1), "test1", "Exchange Rate", "description", true, Option(s"${now.secondOfMinute.getAsString} ${now.minuteOfHour.getAsString} ${now.hourOfDay.getAsString} * * ?"), TimeFrame.DAY_TODAY, Map())))

    val runDrop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    runDrop.waterfallDrop must_== TestPassThroughWaterfallDrop(Some(LocalDate.now.toDateTimeAtStartOfDay))

    run must_== true
  }

  def runJobImmediatelyWithSpecifiedDate = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))

    var run: Boolean = false
    val completionFunc = (job: Option[DropJob]) => {
      run = true
    }

    val request = RunJobImmediately(1, runDate = Some(LocalDate.lastWeek), completionFunc)
    probe.send(actor, request)
    val getJobForCompletion = jobDatabaseManager.expectMsgClass(classOf[GetJobForCompletion])

    val now = DateTime.now(ISOChronology.getInstanceUTC) + Period.seconds(3)
    getJobForCompletion.completionFunction(Some(DropJob(Some(1), "test1", "Exchange Rate", "description", true, Option(s"${now.secondOfMinute.getAsString} ${now.minuteOfHour.getAsString} ${now.hourOfDay.getAsString} * * ?"), TimeFrame.DAY_TODAY, Map())))

    val runDrop = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    runDrop.waterfallDrop must_== TestPassThroughWaterfallDrop(Some(LocalDate.lastWeek.toDateTimeAtStartOfDay))

    run must_== true
  }

  private def createStartJob(frame: TimeFrame.TimeFrame = TimeFrame.DAY_TODAY, runDate: Option[LocalDate] = None): StartJob = {
    val now = DateTime.now(ISOChronology.getInstanceUTC) + Period.seconds(3)
    StartJob(
      1,
      DropJob(Some(1), "test1", "Exchange Rate", "description", true, Option(s"${now.secondOfMinute.getAsString} ${now.minuteOfHour.getAsString} ${now.hourOfDay.getAsString} * * ?"), frame, Map()),
      runDate
    )
  }
}
