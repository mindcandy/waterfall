package com.mindcandy.waterfall

import java.util.UUID

import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog }
import com.mindcandy.waterfall.actor.{ JobDatabaseManager, TimeFrame }
import com.mindcandy.waterfall.config.DatabaseConfig
import com.mindcandy.waterfall.service.DB
import org.joda.time.DateTime

trait TestDatabase {

  def newDB = new DB(DatabaseConfig(s"jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1"))

  def testDropJob1 = DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map(), false)
  def testDropJob2 = DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", false, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map(), true)

  def testDropJobs = List(
    testDropJob1, testDropJob2
  )

  // As the actual reference time in the method may be just a few seconds
  // later than the passed in one, it's fine in the test
  private[this] val now = DateTime.now
  private[this] val beforeReference1 = now - 1.hour
  private[this] val beforeReference2 = now - 2.hour
  private[this] val afterReference = now + 1.hour
  val standardException = Some(new IllegalArgumentException("exception"))
  val convertedException = JobDatabaseManager.convertException(standardException)

  def testDropLogs = List(
    DropLog(UUID.fromString("1b18aaa2-b16e-4a24-bf38-b764833e5088"), 1, beforeReference2, Some(afterReference), Some("log"), None),
    DropLog(UUID.fromString("d0f82eac-f0e4-4794-96af-9fc82926bedb"), 1, beforeReference2, Some(beforeReference1), Some("log"), None),
    DropLog(UUID.fromString("8a75cf7b-db15-4add-879c-c3170e3125ab"), 1, afterReference, None, Some("log"), None),
    DropLog(UUID.fromString("07c72325-ed94-473a-82d3-ba7492cd7a0e"), 1, beforeReference2, None, Some("log"), None),
    DropLog(UUID.fromString("730c7f38-f911-41d7-8c94-9b6b819e8ef3"), 1, beforeReference2, Some(afterReference), None, convertedException),
    DropLog(UUID.fromString("c566c6c9-936c-40df-a1c0-c80ef228c620"), 1, beforeReference2, Some(beforeReference1), None, convertedException),
    DropLog(UUID.fromString("e4022b66-4985-451a-a548-eb2e5e30b609"), 1, afterReference, None, None, convertedException),
    DropLog(UUID.fromString("bcb9b181-2540-43b1-916c-7848775db7e9"), 1, beforeReference2, None, None, convertedException),
    DropLog(UUID.fromString("26413658-20e9-4e49-bd41-23d4b78cf0f2"), 2, beforeReference2, Some(afterReference), Some("log"), None),
    DropLog(UUID.fromString("a4974b43-102d-4f5e-ac4d-5532bf3ea87a"), 2, beforeReference2, Some(beforeReference1), Some("log"), None),
    DropLog(UUID.fromString("5576f977-de25-4346-9f45-2545f3e238be"), 2, afterReference, None, Some("log"), None),
    DropLog(UUID.fromString("00bc951e-ecf2-4144-941f-1f21058e29e0"), 2, beforeReference2, None, Some("log"), None),
    DropLog(UUID.fromString("08502e67-8fd4-441f-bc75-91801e1577b2"), 2, beforeReference2, Some(afterReference), None, convertedException),
    DropLog(UUID.fromString("24d4d7ee-4440-45f1-9527-4453eb70a6e0"), 2, beforeReference2, Some(beforeReference1), None, convertedException),
    DropLog(UUID.fromString("7fdf3abb-916b-4266-a341-e48f534bf5ff"), 2, afterReference, None, None, convertedException),
    DropLog(UUID.fromString("c8ad6ada-f29f-4961-a695-380c442cd1da"), 2, beforeReference2, None, None, convertedException)
  )

  def testDatabaseWithJobsAndLogs = {
    val db = newDB
    db.create(db.allTables)
    db.insert(db.dropJobs, testDropJobs)
    db.insert(db.dropLogs, testDropLogs)
    db
  }

  def testDatabaseWithJobs = {
    val db = newDB
    db.create(db.allTables)
    db.insert(db.dropJobs, testDropJobs)
    db
  }
}
