package org.apache.spark.sql.almondinternals

import org.apache.log4j.{Category, FileAppender, Logger}

import java.io.File

import scala.collection.JavaConverters._

object Log4jFile {

  def logFile(clazz: Class[_]): Option[File] = {

    def appenders(log: Category): Iterator[Any] =
      if (log == null)
        Iterator.empty
      else
        log.getAllAppenders.asScala ++ appenders(log.getParent)

    // FileAppender also matches its subclasses, like RollingFileAppender
    appenders(Logger.getLogger(clazz)).collectFirst {
      case fa: FileAppender if fa.getFile != null => new File(fa.getFile)
    }
  }

}
