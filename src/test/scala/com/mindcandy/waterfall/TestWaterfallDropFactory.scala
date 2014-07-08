package com.mindcandy.waterfall

import com.mindcandy.waterfall.drop.PassThroughWaterfallDrop
import com.mindcandy.waterfall.intermediate.MemoryIntermediate
import com.mindcandy.waterfall.io.{ BaseIOConfig, MemoryIO }
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import org.joda.time.DateTime
import scala.util.{ Failure, Try }

case class TestPassThroughWaterfallDrop() extends PassThroughWaterfallDrop[PlainTextFormat] {
  val fileUrl: String = "memory:intermediate"
  override val sharedIntermediate = MemoryIntermediate[PlainTextFormat](fileUrl)
  val sharedIntermediateFormat = PlainTextFormat.format
  val source = MemoryIO[PlainTextFormat](BaseIOConfig("memory:source"))
  val sink = MemoryIO[PlainTextFormat](BaseIOConfig("memory:sink"))
}

case class TestFailingPassThroughWaterfallDrop() extends PassThroughWaterfallDrop[PlainTextFormat] {
  val fileUrl: String = "memory:intermediate"
  override val sharedIntermediate = MemoryIntermediate[PlainTextFormat](fileUrl)
  val sharedIntermediateFormat = PlainTextFormat.format
  val source = MemoryIO[PlainTextFormat](BaseIOConfig("memory:source"))
  val sink = MemoryIO[PlainTextFormat](BaseIOConfig("memory:sink"))
  override def transform(sourceInter: Intermediate[PlainTextFormat], sinkInter: Intermediate[PlainTextFormat]): Try[Unit] = Failure(new Exception("this will always fail"))
}

class TestWaterfallDropFactory extends WaterfallDropFactory {
  def getDropByUID(dropUID: DropUID, date: Option[DateTime] = None, configuration: Map[String, String] = Map()): Option[WaterfallDrop[_ <: AnyRef, _ <: AnyRef]] = dropUID match {
    case "test1" => Some(TestPassThroughWaterfallDrop())
    case "test2" => Some(TestFailingPassThroughWaterfallDrop())
    case _ => None
  }
}
