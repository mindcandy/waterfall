package com.mindcandy.waterfall.drop

import com.mindcandy.waterfall._

import scala.util.{ Success, Try }

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