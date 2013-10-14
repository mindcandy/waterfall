package com.mindcandy.waterfall.io

import scala.io.Source
import scala.language.implicitConversions

trait IntermediateFormat[A] {
  implicit def toConverter(value: Seq[String]): A
  implicit def fromConverter(value: A): Seq[String]
}

sealed trait Intermediate[A] {
  def url: String
  def read(implicit converter: Seq[String] => A): Iterator[A]
  def write(stream: Iterator[A])(implicit converter: A => Seq[String]): Unit
}

case class MemoryIntermediate[A](url: String) extends Intermediate[A] {
  var data = Seq[Seq[String]]()

  def read(implicit converter: Seq[String] => A): Iterator[A] = data.map(converter).iterator
  def write(value: Iterator[A])(implicit converter: A => Seq[String]): Unit = data = value.map{ converter(_) }.toSeq
}