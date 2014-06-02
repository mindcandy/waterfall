package com.mindcandy.waterfall.drop

import WaterfallDropFactory._
import com.mindcandy.waterfall.PlainTextFormat
import com.mindcandy.waterfall.MemoryIntermediate
import com.mindcandy.waterfall.io.MemoryIO
import com.mindcandy.waterfall.io.BaseIOConfig
import org.joda.time.DateTime

case class TestPassThroughWaterfallDrop() extends PassThroughWaterfallDrop[PlainTextFormat] {
  val fileUrl: String = "memory:intermediate"
  override val sharedIntermediate = MemoryIntermediate[PlainTextFormat](fileUrl)
  val sharedIntermediateFormat = PlainTextFormat.format
  val source = MemoryIO[PlainTextFormat](BaseIOConfig("memory:source"))
  val sink = MemoryIO[PlainTextFormat](BaseIOConfig("memory:sink"))
}


class TestWaterfallDropFactory extends WaterfallDropFactory {
  def getDropByUID(dropUID: DropUID, date: Option[DateTime]=None): Option[WaterfallDrop[_, _]] = dropUID match {
    case "test1" => Some(TestPassThroughWaterfallDrop())
    case _ => None
  }
}
