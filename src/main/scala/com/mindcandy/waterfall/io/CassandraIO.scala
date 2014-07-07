package com.mindcandy.waterfall.io

import com.mindcandy.waterfall._
import com.typesafe.scalalogging.slf4j.Logging
import org.joda.time.DateTime
import scala.util.Try
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.{ ConnectionPoolType, CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl }
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.{ ColumnFamily, ConsistencyLevel }
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.serializers.StringSerializer

case class CassandraIOClusterConfig(name: String, keySpace: String, seedHosts: String, localDatacenter: String,
                                    maxConnsPerHost: Int = 3, maxFailoverCount: Int = 3, defaultReadConsistencyLevelPolicy: ConsistencyLevel = ConsistencyLevel.CL_ONE,
                                    defaultWriteConsistencyLevelPolicy: ConsistencyLevel = ConsistencyLevel.CL_LOCAL_QUORUM, nodeDiscoveryType: NodeDiscoveryType = NodeDiscoveryType.RING_DESCRIBE,
                                    connectionPoolType: ConnectionPoolType = ConnectionPoolType.TOKEN_AWARE)
case class CassandraIOConfig(clusterConfig: CassandraIOClusterConfig, columnFamily: String, keyField: String, fieldToColumnMapping: Map[String, String] = Map()) extends IOConfig {
  val url = "cassandra:cluster"
}

case class CassandraIO[A <: AnyRef](config: CassandraIOConfig)
    extends IOSource[A]
    with IOSink[A]
    with Logging {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    // TODO
    Try(())
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    intermediate.read {
      _.foreach { input =>
        val fields = getFields(input)
        val keyValue = fields(config.keyField).toString
        val columnValues = if (config.fieldToColumnMapping.isEmpty) {
          fields - config.keyField
        } else {
          fields -- (fields.keySet &~ config.fieldToColumnMapping.keySet)
        }
        val mutationBatch = keySpace.prepareMutationBatch()
        val columnListMutation = mutationBatch.withRow(columnFamily, keyValue)
        columnValues.foreach {
          case (name, value) =>
            val properName = config.fieldToColumnMapping.get(name).getOrElse(name)
            value match {
              case b: Boolean => columnListMutation.putColumn(properName, b)
              case i: Int => columnListMutation.putColumn(properName, i)
              case f: Float => columnListMutation.putColumn(properName, f)
              case d: BigDecimal => columnListMutation.putColumn(properName, d.toFloat)
              case timestamp: DateTime => columnListMutation.putColumn(properName, timestamp.toDate)
              case _ => columnListMutation.putColumn(properName, value.toString)
            }
        }
        mutationBatch.execute()
      }
    }
  }

  def getFields(caseClass: A) =
    (Map[String, Any]() /: caseClass.getClass.getDeclaredFields) { (result, field) =>
      field.setAccessible(true)
      if (field.getName.startsWith("$")) { // eliminate compiler generated fields
        result
      } else {
        result + (field.getName -> field.get(caseClass))
      }
    }

  val cassandraContext = {
    val context = new AstyanaxContext.Builder()
      .forCluster(config.clusterConfig.name)
      .forKeyspace(config.clusterConfig.keySpace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(config.clusterConfig.nodeDiscoveryType)
        .setConnectionPoolType(config.clusterConfig.connectionPoolType)
        .setDefaultReadConsistencyLevel(config.clusterConfig.defaultReadConsistencyLevelPolicy)
        .setDefaultWriteConsistencyLevel(config.clusterConfig.defaultWriteConsistencyLevelPolicy)
      )
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(config.clusterConfig.name)
        .setPort(9160)
        .setMaxConnsPerHost(config.clusterConfig.maxConnsPerHost)
        .setMaxFailoverCount(config.clusterConfig.maxFailoverCount)
        .setSeeds(config.clusterConfig.seedHosts)
        .setLocalDatacenter(config.clusterConfig.localDatacenter)
      )
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
    context.start()
    context
  }
  val keySpace = cassandraContext.getClient
  val columnFamily = ColumnFamily.newColumnFamily(config.columnFamily, StringSerializer.get(), StringSerializer.get())
}