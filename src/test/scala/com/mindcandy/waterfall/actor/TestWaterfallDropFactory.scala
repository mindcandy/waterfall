package com.mindcandy.waterfall.actor

import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.WaterfallDropFactory._
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.PassThroughWaterfallDrop
import com.mindcandy.waterfall.PlainTextFormat
import com.mindcandy.waterfall.MemoryIntermediate
import com.mindcandy.waterfall.io.MemoryIO
import com.mindcandy.waterfall.io.BaseIOConfig

case class TestPassThroughWaterfallDrop() extends PassThroughWaterfallDrop[PlainTextFormat] {
  val fileUrl: String = "memory:intermediate"
  override val sharedIntermediate = MemoryIntermediate[PlainTextFormat](fileUrl)
  val sharedIntermediateFormat = PlainTextFormat.format
  val source = MemoryIO[PlainTextFormat](BaseIOConfig("memory:source"))
  val sink = MemoryIO[PlainTextFormat](BaseIOConfig("memory:sink"))
}


object TestWaterfallDropFactory extends WaterfallDropFactory {
  def getDropByUID(dropUID: DropUID): Option[WaterfallDrop[_, _]] = dropUID match {
    case "test1" => Some(TestPassThroughWaterfallDrop())
    case _ => None
  }
}