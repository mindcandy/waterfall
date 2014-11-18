package com.mindcandy.waterfall.service

import java.sql.Timestamp

import argonaut.Argonaut._
import argonaut._
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory._
import com.mindcandy.waterfall.actor.{ LogStatus, TimeFrame }
import com.mindcandy.waterfall.config.{ DatabaseConfig, DatabaseContainer }
import org.joda.time.DateTime
import org.quartz.CronExpression

class DB(val config: DatabaseConfig) extends DatabaseContainer {
  val driver = config.driver
  import com.mindcandy.waterfall.actor.Protocol._
  import driver.simple._

  import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

  val db = Database.forURL(
    config.url, config.username, config.password, driver = config.driverClass
  )

  implicit val dateTimeColumnType = MappedColumnType.base[DateTime, Timestamp](
    { dt => new Timestamp(dt.getMillis) },
    { ts => new DateTime(ts) }
  )

  class DropLogs(tag: Tag) extends Table[DropLog](tag, "drop_log") {
    def runUID = column[RunUID]("run_id", O.PrimaryKey)
    def jobID = column[JobID]("job_id", O.NotNull)
    def startTime = column[DateTime]("start_time", O.NotNull)
    def endTime = column[Option[DateTime]]("end_time")
    def content = column[Option[String]]("content", O.DBType("TEXT"))
    def exception = column[Option[String]]("exception", O.DBType("TEXT"))
    def * =
      (runUID, jobID, startTime, endTime, content, exception) <>
        (DropLog.tupled, DropLog.unapply)
    def job_fk = foreignKey("drop_log_job_id_fk", jobID, dropJobs)(_.jobID)
  }

  val dropLogs = TableQuery[DropLogs]
  // Notice that sorting on runUID is by its string representation, not UUID.
  val dropLogsSorted = dropLogs.sortBy(log => (log.startTime.desc, log.jobID, log.runUID))

  implicit val timeFrameColumnType = MappedColumnType.base[TimeFrame.TimeFrame, String](
    { tf => tf.toString },
    { ts => TimeFrame.withName(ts) }
  )

  implicit val MapStringStringColumnType = MappedColumnType.base[Map[String, String], String](
    { m => m.asJson.nospaces },
    { ts =>
      val opt = Parse.decodeOption[Map[String, String]](ts)
      // TODO(deo.liang): the stored value should be a valid json string, probably we can just do Parse.decode
      opt.getOrElse(Map[String, String]())
    }
  )

  class DropJobs(tag: Tag) extends Table[DropJob](tag, "drop_job") {
    def jobID = column[JobID]("job_id", O.PrimaryKey, O.AutoInc)
    def dropUID = column[String]("drop_uid", O.NotNull)
    def name = column[String]("name", O.NotNull)
    def description = column[String]("description", O.NotNull)
    def enabled = column[Boolean]("enabled", O.NotNull)
    def cron = column[String]("cron", O.Nullable)
    def timeFrame = column[TimeFrame.TimeFrame]("time_frame", O.NotNull)
    // configuration stored as a json string
    def configuration = column[Map[String, String]]("configuration", O.NotNull)
    def parallel = column[Boolean]("parallel", O.NotNull)
    def * =
      (jobID.?, dropUID, name, description, enabled, cron.?, timeFrame, configuration, parallel) <>
        (DropJob.tupled, DropJob.unapply)
  }

  class DropJobDependencies(tag: Tag) extends Table[DropJobDependency](tag, "drop_job_dependency") {
    def initiatorJobID = column[JobID]("initiator_job_id")
    def dependantJobID = column[JobID]("dependant_job_id")
    def pk = primaryKey("pk_drop_job_dependency", (initiatorJobID, dependantJobID))
    def dependantFk = foreignKey("DROP_JOB_DEPENDANT_FK", dependantJobID, dropJobs)(_.jobID)
    def initiatorFk = foreignKey("DROP_JOB_initiator_FK", initiatorJobID, dropJobs)(_.jobID)
    def * = (initiatorJobID, dependantJobID) <> (DropJobDependency.tupled, DropJobDependency.unapply)
  }

  val dropJobs = TableQuery[DropJobs]
  val dropJobsSorted = dropJobs.sortBy(job => (job.dropUID, job.jobID))
  val dropJobDependencies = TableQuery[DropJobDependencies]
  val allTables = Seq(dropJobs, dropLogs, dropJobDependencies)

  def selectDropDependants(jobID: JobID): List[DropJob] = {
    (for {
      dependency <- dropJobDependencies if dependency.initiatorJobID === jobID
      dependant <- dropJobs if dependant.jobID === dependency.dependantJobID
    } yield (dependant)).list
  }

//  def selectDropLog(jobID: JobID): Option[DropJob] = {
//    val test = (for {
//      jobWithInits <- dropJobs leftJoin dropJobDependencies on (_.jobID === _.dependantJobID)
//    } yield jobWithInits)
//      .groupBy(_._1)
//      .map { case (job, results) => (results.map(_._1), results.length) }
//
//    //      .groupBy(_._1.jobID)
//
//    None
//  }
  //  def selectDropDependantss(jobID: JobID): List[DropJob] = {
  //    (for {
  //      dropJob <- dropJobs if dropJob.jobID === jobID
  //      dependancy <- dropJobDependencies if dependancy.initiatorJobID === dropJob.jobID
  //      dependant <- dropJobs if dependant.jobID === dependancy.dependantJobID
  //    } yield (dropJob, dependant)).list.groupBy(_._1)
  //  }

  def maybeExists(dropJob: DropJob): Option[DropJob] =
    dropJob.jobID.flatMap(jid => dropJobs.filter(_.jobID === jid).firstOption)

  def insertAndReturnDropJob(dropJob: DropJob): Option[DropJob] = {
    (dropJobs returning dropJobs.map(_.jobID) into ((job, id) => Some(job.copy(jobID = Some(id))))) += dropJob
  }

  def insertAndReturnDependency(dependency: DropJobDependency): Option[DropJobDependency] = {
    if ((dropJobDependencies += dependency) > 0) Option(dependency) else None
  }

  def updateAndReturnDropJob(dropJob: DropJob): Option[DropJob] = {
    val job = dropJobs.filter(_.jobID === dropJob.jobID)
    job.update(dropJob)
    job.firstOption
  }

  def updateDropLog(runUID: RunUID, endTime: DateTime, logOutput: Option[String], exception: Option[String]) = {
    dropLogs
      .filter(_.runUID === runUID)
      .map(log => (log.endTime, log.content, log.exception))
      .update((Some(endTime), logOutput, exception))
  }

  def insertOrUpdateDropJob(dropJob: DropJob): Option[DropJob] = {
    dropJob.cron match {
      case Some(cron) =>
        CronExpression.isValidExpression(cron) match {
          case false => None
          case true => maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturnDropJob(dropJob))
        }
      case None =>
        maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturnDropJob(dropJob))
    }
    //    (dropJob.cron, dropJob.parent) match {
    //      case (Some(cron), None) =>
    //        CronExpression.isValidExpression(cron) match {
    //          case false => None
    //          case true => maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturnDropJob(dropJob))
    //        }
    //      case (None, Some(initiator)) =>
    //        maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturnDropJob(dropJob))
    //      case _ =>
    //        None
    //    }
  }

  def insertOrUpdateDependencyDropJob(initiatorJobID: JobID, dependencyDropJob: DropJob): Option[DropJob] = {
    insertOrUpdateDropJob(dependencyDropJob).flatMap { job =>
      insertAndReturnDependency(DropJobDependency(initiatorJobID, job.jobID.get)).map(dependency => job)
    }
  }

  val defaultSelectDropLogLimit = 100
  val defaultSelectDropLogOffset = 0

  def selectDropLog(jobID: Option[JobID], period: Option[Int], status: Option[LogStatus.LogStatus], dropUID: Option[DropUID], limit: Option[Int], offset: Option[Int]): List[DropLog] = {
    type LogQuery = Query[(DropLogs, DropJobs), (DropLog, DropJob), Seq]
    val logJoin = for {
      job <- dropJobs
      log <- dropLogsSorted if job.jobID === log.jobID
    } yield (log, job)

    val filterByTime: Option[LogQuery => LogQuery] = period.map(p => { q: LogQuery =>
      val timeFrom = DateTime.now - p.hour
      q.filter {
        case (log, job) =>
          (log.endTime.isDefined && log.endTime >= timeFrom) || (log.endTime.isEmpty && log.startTime >= timeFrom)
      }
    })
    val filterByJobID: Option[LogQuery => LogQuery] = jobID.map(id => { q: LogQuery =>
      q.filter { case (log, job) => log.jobID === id }
    })
    val filterByDropUID: Option[LogQuery => LogQuery] = dropUID.map(uid => { q: LogQuery =>
      q.filter { case (log, job) => job.dropUID === uid }
    })
    val filterByStatus: Option[LogQuery => LogQuery] = status.map(s => { q: LogQuery =>
      s match {
        case LogStatus.FAILURE => q.filter { case (log, job) => log.exception.isDefined }
        case LogStatus.SUCCESS => q.filter { case (log, job) => log.exception.isEmpty }
        case LogStatus.RUNNING => q.filter { case (log, job) => log.endTime.isEmpty }
      }
    })

    val allFilters: List[LogQuery => LogQuery] = List(
      filterByTime, filterByJobID, filterByDropUID, filterByStatus
    ).flatten

    // Apply the functions, if any, to the query object
    allFilters
      .foldLeft(logJoin)((logs, func) => func(logs))
      .drop(offset.getOrElse(defaultSelectDropLogOffset))
      .take(limit.getOrElse(defaultSelectDropLogLimit))
      .list
      .map(_._1)
  }
}
