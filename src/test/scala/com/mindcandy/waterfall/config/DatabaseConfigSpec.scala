package com.mindcandy.waterfall.config

import com.mindcandy.waterfall.TestDatabase
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog }
import com.mindcandy.waterfall.actor.TimeFrame
import org.joda.time.DateTime
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification

import scala.slick.driver.JdbcDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession
import scala.slick.jdbc.meta.MTable

trait TestData {

  val oneDropLog = DropLog(
    None, 1, new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None)
  val oneDropJob = DropJob(
    None, "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY,
    Map[String, String]("configFile" -> "/adx/config.properties"))
}

class DatabaseContainerSpec
    extends Specification
    with Grouped
    with TestData
    with TestDatabase {

  def is = s2"""
  DB test

  ==============================================================================

    create new database ${createNewDatabase.e1}

    create new database but not overwrite ${createNewDatabaseNotOverwrite.e1}

    insert 1 row into database ${insertToDatabase.e1}
    inserted data is correct ${insertToDatabase.e2}

    insert 2 rows into database ${insertTwoToDatabase.e1}
    inserted data is correct ${insertTwoToDatabase.e2}

    overwrite existed database ${overwriteExistDatabase.e1}
    not overwrite existed database ${notOverwriteExistDatabase.e1}

    create tables in new database ${createTablesInNewDB.e1}

    create tables and overwrite existed ones
      tables created successfully ${createTablesOverwrite.e1}
      table 1 is empty ${createTablesOverwrite.e2}
      table 2 is empty ${createTablesOverwrite.e3}

    successfully insert DropLog into DROP_LOG table ${insertDropLog.e1}
    inserted DropLog is correct ${insertDropLog.e2}
    
    select DropLog
      select all have correct record size ${selectDropLog.e1}
      select jobID=1 have correct record size${selectDropLog.e2}
      select jobID have correct jobID ${selectDropLog.e3}
      select newer than 1 hour ago have correct record size ${selectDropLog.e4}
      select newer than 10 hour ago have correct record size ${selectDropLog.e5}
      select failed log have correct record size ${selectDropLog.e6}
      select failed log all have exception field ${selectDropLog.e7}
      select successful log have corect record size ${selectDropLog.e8}
      select successful log don't have exception field ${selectDropLog.e9}
      select successful log no older than 1 hour for jobID=1 ${selectDropLog.e10}
  """

  def createNewDatabase = new group {
    val db = newDB
    db.create(db.all)
    val isTablesExist: Boolean = db.executeInSession {
      !MTable.getTables(db.dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(db.dropJobs.baseTableRow.tableName).list.isEmpty
    }
    e1 := isTablesExist must beTrue
  }

  def createNewDatabaseNotOverwrite = new group {
    val db = newDB
    db.createIfNotExists(db.dropJobs)
    val tableName = db.dropJobs.baseTableRow.tableName
    val isTableExists: Boolean = db.executeInSession {
      !MTable.getTables(tableName).list.isEmpty
    }
    e1 := isTableExists must beTrue
  }

  def insertToDatabase = new group {
    val db = newDB
    db.create(db.dropJobs)
    val numberOfInsert = db.insert(db.dropJobs, oneDropJob)
    val insertedData = db.db.withDynSession {
      db.dropJobs.list
    }

    e1 := numberOfInsert must_== 1
    e2 := insertedData must_== List(
      DropJob(Some(1), "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties")))
  }

  def insertTwoToDatabase = new group {
    val db = newDB
    db.create(db.dropJobs)
    val data = List(
      oneDropJob,
      DropJob(None, "test2", "test", "description", false, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map()))
    val numberOfInsert = db.insert(db.dropJobs, data)
    val insertedData = db.db.withDynSession {
      db.dropJobs.list
    }

    e1 := numberOfInsert must_== Some(2)
    e2 := insertedData must_== List(
      DropJob(Some(1), "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties")),
      DropJob(Some(2), "test2", "test", "description", false, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map()))
  }

  def overwriteExistDatabase = new group {
    val db = newDB
    db.create(db.dropJobs)
    db.insert(db.dropJobs, oneDropJob)
    db.create(db.dropJobs)
    val insertedData = db.db.withDynSession {
      db.dropJobs.list
    }

    e1 := insertedData.isEmpty must beTrue
  }

  def notOverwriteExistDatabase = new group {
    val db = newDB
    db.create(db.dropJobs)
    db.insert(db.dropJobs, oneDropJob)
    db.createIfNotExists(db.dropJobs)
    val insertedData = db.db.withDynSession {
      db.dropJobs.list
    }

    e1 := insertedData.isEmpty must beFalse
  }

  def createTablesInNewDB = new group {
    val db = newDB
    db.createIfNotExists(db.all)
    val isTablesCreated = db.db.withDynSession {
      !MTable.getTables(db.dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(db.dropJobs.baseTableRow.tableName).list.isEmpty
    }

    e1 := isTablesCreated must beTrue
  }

  def createTablesOverwrite = new group {
    val db = newDB
    db.create(db.all)
    db.insert(db.dropJobs, oneDropJob)
    db.create(db.all)
    val isTablesCreated = db.db.withDynSession {
      !MTable.getTables(db.dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(db.dropJobs.baseTableRow.tableName).list.isEmpty
    }

    val dataDropJobs = db.executeInSession(db.dropJobs.list)
    val dataDropLogs = db.executeInSession(db.dropLogs.list)
    e1 := isTablesCreated must beTrue
    e2 := dataDropJobs must_== List()
    e3 := dataDropLogs must_== List()
  }

  def insertDropLog = new group {
    val db = newDB
    db.create(db.all)
    db.insert(db.dropJobs, oneDropJob)
    val numOfInsert = db.insert(db.dropLogs, oneDropLog)
    val insertedData = db.db.withDynSession {
      db.dropLogs.list
    }

    e1 := numOfInsert must_== 1
    e2 := insertedData must_== List(
      DropLog(Some(1), 1, new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None))
  }

  def selectDropLog = new group {
    val db = testDatabase
    e1 := db.executeInSession(db.selectDropLog(None, None, None)).size must_== 16
    e2 := db.executeInSession(db.selectDropLog(Some(1), None, None)).size must_== 8
    e3 := db.executeInSession(db.selectDropLog(Some(1), None, None)).count(_.jobID == 1) must_== 8
    e4 := db.executeInSession(db.selectDropLog(None, Some(1), None)).size must_== 8
    e5 := db.executeInSession(db.selectDropLog(None, Some(10), None)).size must_== 16
    e6 := db.executeInSession(db.selectDropLog(None, None, Some(true))).size must_== 8
    e7 := db.executeInSession(db.selectDropLog(None, None, Some(true))).count(_.exception.isDefined) must_== 8
    e8 := db.executeInSession(db.selectDropLog(None, None, Some(false))).size must_== 8
    e9 := db.executeInSession(db.selectDropLog(None, None, Some(false))).count(_.exception.isEmpty) must_== 8
    e10 := db.executeInSession(db.selectDropLog(Some(1), Some(1), Some(true))).size must_== 2
  }
}
