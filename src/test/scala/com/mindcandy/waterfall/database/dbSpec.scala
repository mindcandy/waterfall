package com.mindcandy.waterfall.database

import java.nio.file.{ Files, Paths }

import com.mindcandy.waterfall.actor.Protocol.{ DropLog, dropLogs, DropJob, dropJobs }
import com.mindcandy.waterfall.actor.TimeFrame
import org.joda.time.DateTime
import org.specs2.specification.{ AfterExample, Grouped }
import org.specs2.specification.script.Specification

import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession
import scala.slick.driver.SQLiteDriver.simple._

trait TestData {
  def db = new DB("jdbc:sqlite:dbspec.db")
  val oneDropLog = DropLog(
    Some(1), "test", new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None)
  val oneDropJob = DropJob(
    Some(1), "test", "test", true, "0 2 * * * ?", TimeFrame.DAY_YESTERDAY,
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

    successfully insert DropJob into DropInfo table ${insertDropJob.e1}
    inserted DropJob is correct ${insertDropJob.e2}
  """

  def after() = Files.delete(Paths.get("dbspec.db"))

  def createNewDatabase = new group {
    val logDB = db
    logDB.create(dropLogs)
    val tableName = dropLogs.baseTableRow.tableName
    val isTableExists: Boolean = logDB.db.withDynSession {
      !MTable.getTables(tableName).list.isEmpty
    }
    e1 := isTableExists must beTrue
  }

  def createNewDatabaseNotOverwrite = new group {
    val logDB = db
    logDB.createIfNotExists(dropLogs)
    val tableName = dropLogs.baseTableRow.tableName
    val isTableExists: Boolean = logDB.db.withDynSession {
      !MTable.getTables(tableName).list.isEmpty
    }
    e1 := isTableExists must beTrue
  }

  def insertToDatabase = new group {
    val logDB = db
    logDB.create(dropLogs)
    val numberOfInsert = logDB.insert(dropLogs, oneDropLog)
    val insertedData = logDB.db.withDynSession {
      dropLogs.list
    }

    e1 := numberOfInsert must_== 1
    e2 := insertedData must_== List(
      DropLog(Some(1), "test", new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None))
  }

  def insertTwoToDatabase = new group {
    val logDB = db
    logDB.create(dropLogs)
    val data = List(
      oneDropLog,
      DropLog(Some(1), "test2", new DateTime(2014, 8, 6, 9, 30), Some(new DateTime(2014, 8, 6, 10, 30)), None, Some("exception msg")))
    val numberOfInsert = logDB.insert(dropLogs, data)
    val insertedData = logDB.db.withDynSession {
      dropLogs.list
    }

    e1 := numberOfInsert must_== Some(2)
    e2 := insertedData must_== List(
      DropLog(Some(1), "test", new DateTime(2014, 8, 6, 9, 30), None, Some("a test message"), None),
      DropLog(Some(1), "test2", new DateTime(2014, 8, 6, 9, 30), Some(new DateTime(2014, 8, 6, 10, 30)), None, Some("exception msg")))
  }

  def overwriteExistDatabase = new group {
    val logDB = db
    logDB.create(dropLogs)
    logDB.insert(dropLogs, oneDropLog)
    logDB.create(dropLogs)
    val insertedData = logDB.db.withDynSession {
      dropLogs.list
    }

    e1 := insertedData.isEmpty must beTrue
  }

  def notOverwriteExistDatabase = new group {
    val logDB = db
    logDB.create(dropLogs)
    logDB.insert(dropLogs, oneDropLog)
    logDB.createIfNotExists(dropLogs)
    val insertedData = logDB.db.withDynSession {
      dropLogs.list
    }

    e1 := insertedData.isEmpty must beFalse
  }

  def createTablesInNewDB = new group {
    val logDB = db
    logDB.createIfNotExists(List(dropJobs, dropLogs))
    val isTablesCreated = logDB.db.withDynSession {
      !MTable.getTables(dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(dropJobs.baseTableRow.tableName).list.isEmpty
    }

    e1 := isTablesCreated must beTrue
  }

  def createTablesOverwrite = new group {
    val logDB = db
    logDB.create(List(dropJobs, dropLogs))
    logDB.insert(dropJobs, oneDropJob)
    logDB.create(List(dropJobs, dropLogs))
    val isTablesCreated = logDB.db.withDynSession {
      !MTable.getTables(dropLogs.baseTableRow.tableName).list.isEmpty &&
        !MTable.getTables(dropJobs.baseTableRow.tableName).list.isEmpty
    }

    val dataDropJobs = logDB.db.withDynSession {
      dropJobs.list
    }
    val dataDropLogs = logDB.db.withDynSession {
      dropLogs.list
    }
    e1 := isTablesCreated must beTrue
    e2 := dataDropJobs must_== List()
    e3 := dataDropLogs must_== List()
  }

  def insertDropJob = new group {
    val logDB = db
    logDB.create(List(dropJobs, dropLogs))
    val numOfInsert = logDB.insert(dropJobs, oneDropJob)
    val insertedData = logDB.db.withDynSession {
      dropJobs.list
    }

    e1 := numOfInsert must_== 1
    e2 := insertedData must_== List(oneDropJob)
  }
}
