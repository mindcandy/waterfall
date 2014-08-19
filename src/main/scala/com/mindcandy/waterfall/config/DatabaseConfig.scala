package com.mindcandy.waterfall.config

import scala.slick.driver._
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession
import scala.slick.jdbc.meta.MTable

// username and password by default is null instead of empty string, otherwise
// they invalidate the properties parameter used in forURL(), at least when
// using sqlite.
case class DatabaseConfig(url: String, username: String = null, password: String = null) {
  val driver: JdbcDriver = {
    val driverRegex = "^jdbc:([^:]+):.+".r
    val driver = url match {
      case driverRegex(name) =>
        name match {
          case "h2" => H2Driver
          case "postgresql" => PostgresDriver
          case _ => throw new RuntimeException("Driver not understood.")
        }
    }
    driver
  }
}

trait DatabaseContainer {
  val driver: JdbcDriver
  import driver.simple._

  val db: driver.backend.DatabaseDef

  def insert[A](table: TableQuery[_ <: Table[A]], entry: A): Int = db.withDynSession {
    table += entry
  }

  def insert[A](table: TableQuery[_ <: Table[A]], entry: Seq[A]): Option[Int] = db.withDynSession {
    table ++= entry
  }

  def create(table: TableQuery[_ <: Table[_]]) = db.withDynSession {
    val tableName = table.baseTableRow.tableName
    if (!MTable.getTables(tableName).list.isEmpty) table.ddl.drop
    table.ddl.create
  }

  def create(tables: Seq[TableQuery[_ <: Table[_]]]) = db.withDynSession {
    val tablesToBeDropped = tables.filter(t => {
      val tableName = t.baseTableRow.tableName
      !MTable.getTables(tableName).list.isEmpty
    })
    if (!tablesToBeDropped.isEmpty)
      tablesToBeDropped.map(_.ddl).reduce(_ ++ _).drop
    tables.map(_.ddl).reduce(_ ++ _).create
  }

  def createIfNotExists(table: TableQuery[_ <: Table[_]]) = db.withDynSession {
    val tableName = table.baseTableRow.tableName
    if (MTable.getTables(tableName).list.isEmpty) {
      table.ddl.create
    }
  }

  def createIfNotExists(tables: Seq[TableQuery[_ <: Table[_]]]) = db.withDynSession {
    val tablesToBeCreated = tables.filter(t => {
      val tableName = t.baseTableRow.tableName
      MTable.getTables(tableName).list.isEmpty
    })
    if (!tablesToBeCreated.isEmpty)
      tablesToBeCreated.map(_.ddl).reduce(_ ++ _).create
  }

  def executeInSession[T](f: => T): T = db.withDynSession(f)
}
