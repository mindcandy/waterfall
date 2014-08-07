package com.mindcandy.waterfall.actor

import akka.testkit.{ TestProbe, TestKit }
import akka.actor.ActorSystem
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropJobList }
import scala.concurrent.duration._
import com.mindcandy.waterfall.config.JobsDatabaseConfig
import com.mindcandy.waterfall.database
import org.specs2.mock.Mockito

class JobDatabaseManagerSpec
  extends TestKit(ActorSystem("JobDatabaseManagerSpec"))
  with SpecificationLike
  with After
  with NoTimeConversions
  with Mockito {
  override def is = s2"""
    JobDatabaseManager should
      send correct schedule $getSchedule
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  val config = JobsDatabaseConfig(DropJobList(List(
    DropJob(None, "EXRATE", "Exchange Rate", true, "0 1 * * *", TimeFrame.DAY_YESTERDAY, Map()),
    DropJob(None, "ADX", "Adx", true, "0 2 * * *", TimeFrame.DAY_YESTERDAY, Map("configFile" -> "/adx/config.properties"))
  )))

  def getSchedule = {
    val probe = TestProbe()
    val db = mock[database.DB]
    val actor = system.actorOf(JobDatabaseManager.props(config, db))

    probe.send(actor, GetSchedule())

    val expectedMessage = config.dropJobList
    probe.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage
  }
}
