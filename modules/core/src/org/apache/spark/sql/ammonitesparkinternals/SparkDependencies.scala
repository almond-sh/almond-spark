package org.apache.spark.sql.ammonitesparkinternals

import java.net.URI

import coursierapi.{Dependency, Fetch, Module, Repository, ResolutionParams}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Properties.{versionNumberString => scalaVersion}
import scala.util.Try

object SparkDependencies {

  private val sbv =
    scalaVersion
      .split('.')
      .take(2)
      .mkString(".")

  private val sparkHiveClasses =
    List(
      // checking two classes just-in-case, first one is supposed to be deprecated
      "org.apache.spark.sql.hive.HiveContext",
      "org.apache.spark.sql.hive.HiveSessionStateBuilder"
    )

  private def sparkYarnClass = "org.apache.spark.deploy.yarn.Client"
  private def yarnAmIpFilterClass =
    "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
  private def sparkExecutorClassLoaderClass = "org.apache.spark.repl.ExecutorClassLoader"

  private def classFound(className: String): Boolean =
    try {
      Thread.currentThread().getContextClassLoader.loadClass(className)
      true
    }
    catch {
      case _: ClassNotFoundException =>
        false
    }

  def sparkHiveFound(): Boolean =
    sparkHiveClasses.exists(classFound)

  def sparkYarnFound(): Boolean =
    classFound(sparkYarnClass)

  def yarnAmIpFilterFound(): Boolean =
    classFound(yarnAmIpFilterClass)

  def sparkExecutorClassLoaderFound(): Boolean =
    classFound(sparkExecutorClassLoaderClass)

  private def sparkModules(): Seq[String] = {

    val b = new mutable.ListBuffer[String]

    b ++= Seq("core", "sql")

    val cl = Thread.currentThread().getContextClassLoader

    @tailrec
    def addIfClasses(module: String, classNames: List[String]): Unit =
      classNames match {
        case Nil =>
        case h :: t =>
          try {
            cl.loadClass(h)
            b += module
          }
          catch {
            case _: ClassNotFoundException =>
              addIfClasses(module, t)
          }
      }

    def addIfClass(module: String, className: String): Unit =
      addIfClasses(module, className :: Nil)

    addIfClass("yarn", sparkYarnClass)
    addIfClasses("hive", sparkHiveClasses)
    addIfClass("mllib", "org.apache.spark.mllib.optimization.GradientDescent")
    addIfClass("graphx", "org.apache.spark.graphx.Graph")
    addIfClass("streaming", "org.apache.spark.streaming.StreamingContext")

    b.result()
  }

  def stubsDependency = {
    val sv = org.apache.spark.SPARK_VERSION
    val suffix = sv.split('.').take(2) match {
      case Array("0" | "1", _) =>
        sys.error(s"Spark $sv is not supported (Spark >= 2.4 required)")
      case Array("2", n) if Try(n.toInt).toOption.exists(_ <= 3) =>
        sys.error(s"Spark $sv is not supported (Spark >= 2.4 required)")
      case Array("2", _) =>
        "24"
      case Array("3", n) if Try(n.toInt).toOption.exists(_ <= 1) =>
        "30"
      case Array("3" | "4", _) =>
        "32"
      case _ =>
        System.err.println(s"Warning: unrecognized Spark version ($sv), assuming 3.2.x or 4.x")
        "32"
    }
    Dependency.of(
      "sh.almond",
      s"spark-stubs_${suffix}_$sbv",
      Properties.version
    )
  }

  def sparkYarnDependency =
    Dependency.of(
      "org.apache.spark",
      s"spark-yarn_$sbv",
      org.apache.spark.SPARK_VERSION
    )

  def yarnWebProxyDependency = {
    val versionInfo = Thread.currentThread().getContextClassLoader
      .loadClass("org.apache.hadoop.util.VersionInfo")
    val hadoopVersion = versionInfo.getMethod("getVersion").invoke(null).toString
    Dependency
      .of(
        "org.apache.hadoop",
        "hadoop-yarn-server-web-proxy",
        hadoopVersion
      )
      // Spark's hadoop-client-api/runtime already supplies the Hadoop client
      // classes needed by AmIpFilter. Pulling this artifact transitively would
      // add a second, unshaded Hadoop and Jetty stack to the REPL classpath.
      .withTransitive(false)
  }

  def sparkHiveDependency =
    Dependency.of(
      "org.apache.spark",
      s"spark-hive_$sbv",
      org.apache.spark.SPARK_VERSION
    )

  private def sparkBaseDependencies() =
    Seq(
      Dependency.of("org.scala-lang", "scala-library", scalaVersion),
      Dependency.of("org.scala-lang", "scala-reflect", scalaVersion),
      Dependency.of("org.scala-lang", "scala-compiler", scalaVersion),
      stubsDependency // for ExecutorClassLoader
    ) ++
      sparkModules().map { m =>
        Dependency.of("org.apache.spark", s"spark-${m}_$sbv", org.apache.spark.SPARK_VERSION)
      }

  def sparkJars(
    repositories: Seq[Repository],
    resolutionHooks: mutable.Buffer[Fetch => Fetch],
    profiles: Seq[String]
  ): Seq[URI] = {
    val fetch = Fetch.create()
      .addDependencies(sparkBaseDependencies(): _*)
      .withRepositories(repositories: _*)
      .withResolutionParams(
        ResolutionParams.create()
          .forceVersion(Module.of("org.scala-lang", "scala-library"), scalaVersion)
          .forceVersion(Module.of("org.scala-lang", "scala-reflect"), scalaVersion)
          .forceVersion(Module.of("org.scala-lang", "scala-compiler"), scalaVersion)
          .withProfiles(profiles.toSet.asJava)
      )

    resolutionHooks
      .foldLeft(fetch) { case (acc, f) => f(acc) }
      .fetch()
      .asScala
      .toVector
      .map(_.getAbsoluteFile.toURI)
  }
}
