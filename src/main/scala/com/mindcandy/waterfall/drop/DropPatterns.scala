package com.mindcandy.waterfall.drop

import scala.util.{ Success, Try }
import com.mindcandy.waterfall._
import com.mindcandy.waterfall.intermediate.FileIntermediate

trait PassThroughWaterfallDrop[A <: AnyRef] extends WaterfallDrop[A, A] {
  def sharedIntermediate: Intermediate[A]
  def sharedIntermediateFormat: IntermediateFormat[A]

  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  def sourceIntermediateFormat = sharedIntermediateFormat

  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  def sinkIntermediateFormat = sharedIntermediateFormat

  def transform(sourceInter: Intermediate[A], sinkInter: Intermediate[A]): Try[Unit] = Success(())
}