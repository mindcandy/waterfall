package com.mindcandy.waterfall.intermediate

import java.net.URI
import java.nio.charset.Charset
import java.nio.file.{ Files, Paths, StandardOpenOption }

import com.mindcandy.waterfall.{ IOOps, Intermediate, IntermediateFormat, IntermediateOps }
import resource._

import scala.util.Try

/**
 * Read or write data within memory.
 * @param url unused param
 * @tparam A
 */
case class MemoryIntermediate[A <: AnyRef](url: String) extends Intermediate[A] {
  val data = collection.mutable.ArrayBuffer[Seq[String]]()

  def read[B](f: Iterator[A] => Try[B])(implicit format: IntermediateFormat[A]): Try[B] = {
    f(data.map(format.convertTo).iterator)
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = Try {
    data ++= stream.map(format.convertFrom)
    ()
  }

  def getData(): List[Seq[String]] = {
    data.toList
  }

  def clearData() {
    data.clear()
  }
}

case class FileIntermediate[A <: AnyRef](url: String, override val columnSeparator: Option[String] = Option("\t")) extends Intermediate[A] with IOOps[A] with IntermediateOps {

  lazy val path = Paths.get(new URI(url))

  def read[B](f: Iterator[A] => Try[B])(implicit format: IntermediateFormat[A]): Try[B] = {
    val bufferedReader = Try(Files.newBufferedReader(path, Charset.defaultCharset()))
    val managedResource = bufferedReader.map { bufReader =>
      for {
        reader <- managed(bufReader)
      } yield {
        Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).map { line =>
          fromLine(line.get)
        }
      }
    }
    managedResource.flatMap { _.acquireFor(f).convertToTry }
  }

  def write(stream: Iterator[A])(implicit format: IntermediateFormat[A]): Try[Unit] = Try {
    if (!Files.exists(path)) Files.createFile(path)
    for {
      writer <- managed(Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.APPEND))
    } {
      stream.foreach { input =>
        writer.write(toLine(input))
        writer.newLine()
      }
    }
  }
}