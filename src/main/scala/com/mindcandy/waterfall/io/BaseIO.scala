package com.mindcandy.waterfall.io

import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.Intermediate
import com.mindcandy.waterfall.IOConfig
import com.mindcandy.waterfall.IOSource
import com.mindcandy.waterfall.IOSink
import org.apache.commons.vfs2.VFS
import java.io._
import resource._
import com.mindcandy.waterfall.IOOps
import com.mindcandy.waterfall.RowSeparator._
import scala.util.Try
import com.mindcandy.waterfall.IntermediateOps
import com.mindcandy.waterfall.intermediate.{ FileIntermediate, MemoryIntermediate }

case class BaseIOConfig(url: String) extends IOConfig

case class MemoryIO[A <: AnyRef](config: IOConfig)
    extends IOSource[A]
    with IOSink[A] {

  val memoryIntermediate = MemoryIntermediate[A](config.url)

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the MemoryIntermediate
    memoryIntermediate.read(intermediate.write).map { _ =>
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the MemoryIntermediate
    intermediate.read(memoryIntermediate.write).map { _ =>
      logger.info("Store from %s into %s completed".format(intermediate, config))
    }
  }
}

case class FileIO[A <: AnyRef](config: IOConfig, columnSeparator: Option[String] = Option("\t"))
    extends IOSource[A]
    with IOSink[A] {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file reading
    val inputFile = FileIntermediate[A](config.url, columnSeparator)
    inputFile.read(intermediate.write).map { _ =>
      logger.info("Retrieving into %s from %s completed".format(intermediate, config))
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    // reusing the FileIntermediate for file writing
    val outputFile = FileIntermediate[A](config.url, columnSeparator)
    intermediate.read(outputFile.write).map { _ =>
      logger.info("Store from %s into %s completed".format(intermediate, config))
    }
  }
}

case class ApacheVfsIO[A <: AnyRef](config: IOConfig, val columnSeparator: Option[String] = None, val rowSeparator: RowSeparator = NewLine)
    extends IOSource[A]
    with IOSink[A]
    with IOOps[A]
    with IntermediateOps {

  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Try[Unit] = {
    val inputContent = fileContent.map { content =>
      for {
        reader <- managed(new BufferedReader(new InputStreamReader(content.getInputStream())))
      } yield {
        val rawData = Iterator.continually {
          Option(reader.readLine())
        }.takeWhile(_.nonEmpty).flatten
        processRowSeparator(rawData, rowSeparator)
      }
    }

    inputContent.flatMap { resource =>
      resource.acquireFor(intermediate.write).convertToTry.map { _ =>
        logger.info("Retrieving into %s from %s completed".format(intermediate, config))
      }
    }
  }

  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]) = {
    fileContent.map { content =>
      for {
        writer <- managed(new BufferedWriter(new OutputStreamWriter(content.getOutputStream())))
      } {
        intermediate.read { iterator =>
          Try {
            iterator.foreach { input =>
              writer.write(toLine(input))
              rowSeparator match {
                case NewLine => writer.newLine()
                case NoSeparator =>
              }
            }
          }
        }
      }
    }
  }

  private[this] def fileContent = Try {
    val fileObject = VFS.getManager().resolveFile(config.url);
    fileObject.getContent()
  }
}
