package com.mindcandy.waterfall.io

import java.net.InetSocketAddress

import com.datastax.driver.core.policies.AddressTranslater
import com.mindcandy.waterfall._
import com.typesafe.scalalogging.slf4j.Logging
import com.datastax.driver.core.{ BoundStatement, Session, ProtocolVersion, Cluster }
import org.joda.time.DateTime

import scala.util.Try

case class CassandraIOConfig(query: String, fieldSelector: List[String], seedHosts: List[String], addressTranslation: Option[Map[String, String]] = None) extends IOConfig {
  val url = "cassandra:cluster"

  class LookUpAddressTranslator(translations: Map[String, String]) extends AddressTranslater {
    val lookUp: Map[InetSocketAddress, InetSocketAddress] =
      translations
        .collect {
          case (source, destination) => (new InetSocketAddress(source, 9042), new InetSocketAddress(destination, 9042))
        }.toMap
    override def translate(address: InetSocketAddress): InetSocketAddress = {
      lookUp.getOrElse(address, address)
    }
  }

  val lookUpAddressTranslator: Option[LookUpAddressTranslator] = addressTranslation.map(new LookUpAddressTranslator(_))
}

case class CassandraIO[A <: AnyRef](config: CassandraIOConfig)
    extends IOSource[A]
    with IOSink[A]
    with Logging {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    // TODO
    Try(())
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = getSession.flatMap { session =>
    val result = intermediate.read { iterator =>
      Try {
        val statement = session.prepare(config.query)
        iterator.foreach { input =>
          val fields = getFields(input)
          val values = config.fieldSelector.map { field =>
            fields.get(field) match {
              case Some(timestamp: DateTime) => timestamp.toDate
              case Some(value) => value
              case _ => throw new IllegalArgumentException(s"field [$field] in field selector but not in data object with fields ${fields.keys.toList}")
            }
          }
          val boundStatement = new BoundStatement(statement)
          session.execute(boundStatement.bind(values: _*))
        }
      }
    }
    session.close()
    result
  }

  def getFields(caseClass: A) =
    (Map[String, AnyRef]() /: caseClass.getClass.getDeclaredFields) { (result, field) =>
      field.setAccessible(true)
      if (field.getName.startsWith("$")) { // eliminate compiler generated fields
        result
      } else {
        result + (field.getName -> field.get(caseClass))
      }
    }

  val cassandraCluster: Cluster = {
    val builder = Cluster.builder().addContactPoints(config.seedHosts: _*).withProtocolVersion(ProtocolVersion.V2)
    val translatedBuilder = config.lookUpAddressTranslator.fold(builder)(builder.withAddressTranslater(_))
    translatedBuilder.build()
  }
  def getSession: Try[Session] = Try { cassandraCluster.connect() }
}