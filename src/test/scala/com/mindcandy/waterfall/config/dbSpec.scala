package com.mindcandy.waterfall.config

import java.nio.file.{ Files, Paths }

import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog }
import com.mindcandy.waterfall.actor.{ DB, TimeFrame }
import org.joda.time.DateTime
import org.specs2.specification.script.Specification
import org.specs2.specification.{ AfterExample, Grouped }

import scala.slick.driver.JdbcDriver.simple._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession
import scala.slick.jdbc.meta.MTable

trait TestData {
  def db = new DB(DatabaseConfig("jdbc:sqlite:dbspec.db"))
  val oneDropLog = DropLog(
    None, 1, new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None)
  val oneDropJob = DropJob(
    None, "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY,
    Map[String, String]("configFile" -> "/adx/config.properties"))
}

class DBSpec extends Specification with Grouped with AfterExample with TestData {

  def is = sequential ^ s2"""
  DropLogging Database test

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
  """

  def after() = Files.delete(Paths.get("dbspec.db"))

  def createNewDatabase = new group {
    val logDB = db
    logDB.create(logDB.dropLogs)
    val tableName = logDB.dropLogs.baseTableRow.tableName
    val isTableExists: Boolean = logDB.db.withDynSession {
      !MTable.getTables(tableName).list.isEmpty
    }
    e1 := isTableExists must beTrue
  }

  def createNewDatabaseNotOverwrite = new group {
    val logDB = db
    logDB.createIfNotExists(logDB.dropLogs)
    val tableName = logDB.dropLogs.baseTableRow.tableName
    val isTableExists: Boolean = logDB.db.withDynSession {
      !MTable.getTables(tableName).list.isEmpty
    }
    e1 := isTableExists must beTrue
  }

  def insertToDatabase = new group {
    val logDB = db
    logDB.create(logDB.dropJobs)
    val numberOfInsert = logDB.insert(logDB.dropJobs, oneDropJob)
    val insertedData = logDB.db.withDynSession {
      logDB.dropJobs.list
    }

    e1 := numberOfInsert must_== 1
    e2 := insertedData must_== List(
      DropJob(Some(1), "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties")))
  }

  def insertTwoToDatabase = new group {
    val logDB = db
    logDB.create(logDB.dropJobs)
    val data = List(
      oneDropJob,
      DropJob(None, "test2", "test", "description", false, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map()))
    val numberOfInsert = logDB.insert(logDB.dropJobs, data)
    val insertedData = logDB.db.withDynSession {
      logDB.dropJobs.list
    }

    e1 := numberOfInsert must_== Some(2)
    e2 := insertedData must_== List(
      DropJob(Some(1), "test", "test", "description", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties")),
      DropJob(Some(2), "test2", "test", "description", false, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY, Map()))
  }

  def overwriteExistDatabase = new group {
    val logDB = db
    logDB.create(logDB.dropJobs)
    logDB.insert(logDB.dropJobs, oneDropJob)
    logDB.create(logDB.dropJobs)
    val insertedData = logDB.db.withDynSession {
      logDB.dropJobs.list
    }

    e1 := insertedData.isEmpty must beTrue
  }

  def notOverwriteExistDatabase = new group {
    val logDB = db
    logDB.create(logDB.dropJobs)
    logDB.insert(logDB.dropJobs, oneDropJob)
    logDB.createIfNotExists(logDB.dropJobs)
    val insertedData = logDB.db.withDynSession {
      logDB.dropJobs.list
    }

    e1 := insertedData.isEmpty must beFalse
  }

  def createTablesInNewDB = new group {
    val logDB = db
    logDB.createIfNotExists(List(logDB.dropJobs, logDB.dropLogs))
    val isTablesCreated = logDB.db.withDynSession {
      !MTable.getTables(logDB.dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(logDB.dropJobs.baseTableRow.tableName).list.isEmpty
    }

    e1 := isTablesCreated must beTrue
  }

  def createTablesOverwrite = new group {
    val logDB = db
    logDB.create(List(logDB.dropJobs, logDB.dropLogs))
    logDB.insert(logDB.dropJobs, oneDropJob)
    logDB.create(List(logDB.dropJobs, logDB.dropLogs))
    val isTablesCreated = logDB.db.withDynSession {
      !MTable.getTables(logDB.dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(logDB.dropJobs.baseTableRow.tableName).list.isEmpty
    }

    val dataDropJobs = logDB.db.withDynSession {
      logDB.dropJobs.list
    }
    val dataDropLogs = logDB.db.withDynSession {
      logDB.dropLogs.list
    }
    e1 := isTablesCreated must beTrue
    e2 := dataDropJobs must_== List()
    e3 := dataDropLogs must_== List()
  }

  def insertDropLog = new group {
    val logDB = db
    logDB.create(List(logDB.dropLogs, logDB.dropJobs))
    logDB.insert(logDB.dropJobs, oneDropJob)
    val numOfInsert = logDB.insert(logDB.dropLogs, oneDropLog)
    val insertedData = logDB.db.withDynSession {
      logDB.dropLogs.list
    }

    e1 := numOfInsert must_== 1
    e2 := insertedData must_== List(
      DropLog(Some(1), 1, new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None))
  }
}
