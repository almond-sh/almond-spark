package org.apache.spark.sql.almondinternals

import java.io.File
import java.nio.file.Files

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import utest._

object LogFileTests extends TestSuite {

  private def withTmpDir[T](f: File => T): T = {
    val dir = Files.createTempDirectory("almond-spark-tests").toFile
    try f(dir)
    finally {
      Option(dir.listFiles()).toSeq.flatten.foreach(_.delete())
      dir.delete()
    }
  }

  val tests = Tests {

    test("log4j 2 rolling file appender") {
      withTmpDir { dir =>
        val logFile = new File(dir, "spark.log")

        val builder = ConfigurationBuilderFactory.newConfigurationBuilder()
        builder.add(
          builder.newAppender("rolling", "RollingFile")
            .addAttribute("fileName", logFile.getAbsolutePath)
            .addAttribute("filePattern", logFile.getAbsolutePath + ".%i")
            .add(builder.newLayout("PatternLayout"))
            .addComponent(
              builder.newComponent("Policies")
                .addComponent(
                  builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "10MB")
                )
            )
        )
        builder.add(builder.newRootLogger(Level.INFO).add(builder.newAppenderRef("rolling")))
        val ctx = Configurator.initialize(builder.build())
        try {
          // in case the logging context was already initialized
          Configurator.reconfigure(builder.build())
          val found = Log4j2File.logFile(getClass)
          assert(found.map(_.getAbsoluteFile) == Some(logFile.getAbsoluteFile))
        }
        finally Configurator.shutdown(ctx)
      }
    }

    test("log4j 1 file appender") {
      // The Spark versions we compile against here rely on log4j 1.x
      withTmpDir { dir =>
        val logFile = new File(dir, "spark.log")
        val appender = new org.apache.log4j.FileAppender(
          new org.apache.log4j.SimpleLayout,
          logFile.getAbsolutePath
        )
        val root = org.apache.log4j.Logger.getRootLogger
        root.addAppender(appender)
        try {
          val found = NotebookSparkSessionBuilder.logFile(getClass)
          assert(found.map(_.getAbsoluteFile) == Some(logFile.getAbsoluteFile))
        }
        finally {
          root.removeAppender(appender)
          appender.close()
        }
      }
    }
  }
}
