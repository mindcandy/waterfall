package com.mindcandy.waterfall

import java.util.UUID

import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog }
import com.mindcandy.waterfall.actor.{ DB, TimeFrame }
import com.mindcandy.waterfall.config.DatabaseConfig
import org.joda.time.DateTime

trait TestDatabase {

  def newDB = new DB(DatabaseConfig(s"jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1"))

  def testDatabase = {
    val db = newDB
    // As the actual reference time in the method may be just a few seconds
    // later than the passed in one, it's fine in the test
    val now = DateTime.now
    val beforeReference1 = now - 1.hour
    val beforeReference2 = now - 2.hour
    val afterReference = now + 1.hour
    db.create(db.all)
    db.insert(
      db.dropJobs,
      List(
        DropJob(None, "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(None, "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map())
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
    db
  }
}
