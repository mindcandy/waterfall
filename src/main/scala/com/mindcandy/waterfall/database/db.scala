package com.mindcandy.waterfall.database

import scala.slick.driver.SQLiteDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

class DB(url: String) {
  val db = Database.forURL(url, driver = "org.sqlite.JDBC")

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
}
