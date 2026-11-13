package org.apache.spark.sql.almondinternals

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{Appender, Logger => Log4jLogger}
import org.apache.logging.log4j.core.appender.{
  FileAppender,
  MemoryMappedFileAppender,
  RandomAccessFileAppender,
  RollingFileAppender,
  RollingRandomAccessFileAppender
}

import java.io.File

import scala.collection.JavaConverters._

object Log4j2File {

  def logFile(clazz: Class[_]): Option[File] = {

    def appenders(log: Log4jLogger): Iterator[Appender] =
      if (log == null)
        Iterator.empty
      else
        log.getAppenders.asScala.valuesIterator ++ appenders(log.getParent)

    // The various file appenders of log4j 2 don't share a common parent class
    // exposing the file name, so we need to match each of them
    def fileName(appender: Appender): Option[String] =
      appender match {
        case a: FileAppender                    => Option(a.getFileName)
        case a: RollingFileAppender             => Option(a.getFileName)
        case a: RandomAccessFileAppender        => Option(a.getFileName)
        case a: RollingRandomAccessFileAppender => Option(a.getFileName)
        case a: MemoryMappedFileAppender        => Option(a.getFileName)
        case _                                  => None
      }

    // LogManager.getLogger can return loggers of other implementations than log4j-core,
    // if log4j-api is bound to something else (like log4j-to-slf4j)
    LogManager.getLogger(clazz) match {
      case logger: Log4jLogger =>
        appenders(logger).map(fileName).collectFirst { case Some(name) => new File(name) }
      case _ =>
        None
    }
  }

}
