package org.apache.spark.sql.ammonitesparkinternals

import java.io.File
import java.net.{InetAddress, URI, URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import ammonite.interp.api.InterpAPI
import ammonite.repl.api.ReplAPI
import coursier.cputil.ClassPathUtil
import coursierapi.Dependency
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.ConsoleProgressBar

import scala.collection.JavaConverters._

object AmmoniteSparkSessionBuilder {

  private def prettyDir(dir: String): String = {
    val home = sys.props("user.home")

    if (dir.startsWith(home))
      "~" + dir.stripPrefix(home)
    else
      dir
  }

  private def confEnvVars: Seq[String] = {

    val sparkBinaryVersion =
      org.apache.spark.SPARK_VERSION
        .split('.')
        .take(2)
        .mkString(".")

    Seq(
      "SPARK_CONF_DEFAULTS",
      s"SPARK_CONF_DEFAULTS_${sparkBinaryVersion.filter(_ != '.')}"
    )
  }

  def shouldPassToSpark(classpathEntry: URL): Boolean = {
    val isJarFile = classpathEntry.getProtocol == "file" &&
      classpathEntry.getPath.endsWith(".jar") &&
      !classpathEntry.getPath.endsWith("-sources.jar")
    // Should be useful in almond, if one generates a launcher for it with the --standalone option (to embed the
    // dependencies in the launcher).
    // Since https://github.com/coursier/coursier/pull/1024 in coursier, these embedded dependencies should have URLs
    // like jar:file:/…, with possible multiple '!/' in them.
    val isJarInJar = classpathEntry.getProtocol == "jar" &&
      classpathEntry.getFile.endsWith(".jar!/") &&
      !classpathEntry.getPath.endsWith("-sources.jar!/")
    isJarFile || isJarInJar
  }

  // The JVM application class loader exposes the launch class path via its URLs up
  // to Java 8, but stopped being a URLClassLoader in Java 9+. The logic below walks
  // the class loaders the way class-path-inspector does, so that we can still gather
  // the base jars (e.g. the Ammonite / pprint jars the REPL closures need on the
  // executors) on recent JVMs.
  // Based on https://github.com/alexarchambault/class-path-inspector/blob/2f99036618e2b1e1939e557bf0f438b4978fccac/classpath/Inspector.scala

  private lazy val builtinClassLoaderClass =
    try Class.forName("jdk.internal.loader.BuiltinClassLoader")
    catch {
      case _: ClassNotFoundException => null
    }

  private lazy val isBuiltinClassLoader: ClassLoader => Boolean =
    if (builtinClassLoaderClass == null)
      _ => false
    else
      cl => builtinClassLoaderClass.isInstance(cl)

  private lazy val tryGetUrls: ClassLoader => Option[Array[URL]] = {

    // To work fine, this requires
    //   --add-opens java.base/jdk.internal.loader=ALL-UNNAMED

    // originally based on https://stackoverflow.com/questions/49557431/how-to-safely-access-the-urls-of-all-resource-files-in-the-classpath-in-java-9

    val clazz =
      try Class.forName("jdk.internal.loader.URLClassPath")
      catch {
        case _: ClassNotFoundException => null
      }

    val noop = (_: ClassLoader) => None

    if (builtinClassLoaderClass != null && clazz != null) {
      val ucpField =
        try {
          val f = builtinClassLoaderClass.getDeclaredField("ucp")
          f.setAccessible(true)
          f
        }
        catch {
          case _: NoSuchFieldException =>
            null
          case e: Exception
              if e.getClass.getName == "java.lang.reflect.InaccessibleObjectException" =>
            null
        }
      val getURLs =
        try clazz.getMethod("getURLs")
        catch {
          case _: NoSuchMethodException =>
            null
        }

      if (ucpField != null && getURLs != null)
        (cl: ClassLoader) =>
          Option(ucpField.get(cl)).map(getURLs.invoke(_).asInstanceOf[Array[URL]])
      else
        noop
    }
    else
      noop
  }

  private def jarsIn(dir: os.Path): Seq[os.Path] =
    if (os.isDir(dir))
      os.list(dir).filter(_.last.toLowerCase(java.util.Locale.ROOT).endsWith(".jar"))
    else Nil

  private[ammonitesparkinternals] def expandClassPathEntry(entry: String): Seq[os.Path] =
    // The JVM expands "dir/*" class path entries to the JARs of "dir".
    // We also accept "dir/*.jar", which means the same.
    if (entry.endsWith("/*"))
      jarsIn(os.Path(entry.stripSuffix("/*"), os.pwd))
    else if (entry.endsWith("/*.jar"))
      jarsIn(os.Path(entry.stripSuffix("/*.jar"), os.pwd))
    else {
      val path = os.Path(entry, os.pwd)
      if (os.exists(path)) Seq(path)
      else Nil
    }

  private def expandClassPath(input: String): Seq[os.Path] =
    input
      .split(File.pathSeparator)
      .toSeq
      .filter(_.nonEmpty)
      .flatMap(expandClassPathEntry)

  def classpath(cl: ClassLoader): Stream[java.net.URL] =
    if (cl == null)
      Stream()
    else {
      val urls = cl match {
        case ucl: java.net.URLClassLoader =>
          ucl.getURLs.toStream
        case _ =>
          val viaBuiltinOpt =
            if (isBuiltinClassLoader(cl))
              tryGetUrls(cl).map(_.toStream)
            else
              None
          viaBuiltinOpt
            .orElse {
              if (cl.getClass.getName == "jdk.internal.loader.ClassLoaders$AppClassLoader")
                Some {
                  expandClassPath(sys.props.getOrElse("java.class.path", ""))
                    .toStream
                    .map(_.toNIO.toUri.toURL)
                }
              else
                None
            }
            .getOrElse(Stream.empty)
      }

      urls #::: classpath(cl.getParent)
    }

  private lazy val javaDirs = {
    val l = Seq(sys.props("java.home")) ++
      sys.props.get("java.ext.dirs")
        .toSeq
        .flatMap(_.split(File.pathSeparator))
        .filter(_.nonEmpty) ++
      sys.props.get("java.endorsed.dirs")
        .toSeq
        .flatMap(_.split(File.pathSeparator))
        .filter(_.nonEmpty)
    l.map(_.stripSuffix("/") + "/")
  }

  def isJdkJar(uri: URI): Boolean =
    uri.getScheme == "file" && {
      val path = new File(uri).getAbsolutePath
      javaDirs.exists(path.startsWith)
    }

  def trySetProgressBars(sc: SparkContext, value: Option[ConsoleProgressBar]): Boolean = {
    def attempt(methodName: String): Boolean =
      try {
        val method = classOf[org.apache.spark.SparkContext]
          .getDeclaredMethod(
            methodName,
            classOf[Option[Any]]
          )
        method.setAccessible(true)
        method.invoke(sc, value)
        true
      }
      catch {
        case _: NoSuchMethodException =>
          false
      }

    attempt("org$apache$spark$SparkContext$$_progressBar_$eq") || attempt("_progressBar_$eq")
  }

  def tryGetProgressBars(sc: SparkContext): Option[Option[ConsoleProgressBar]] = {
    def attempt(methodName: String): Option[Option[ConsoleProgressBar]] =
      try {
        val method = classOf[org.apache.spark.SparkContext].getDeclaredMethod(methodName)
        method.setAccessible(true)
        Some(method.invoke(sc).asInstanceOf[Option[ConsoleProgressBar]])
      }
      catch {
        case _: NoSuchMethodException =>
          None
      }

    attempt("org$apache$spark$SparkContext$$_progressBar").orElse(attempt("_progressBar"))
  }

  def forceProgressBars(sc: SparkContext): Boolean =
    sc.progressBar.nonEmpty || trySetProgressBars(sc, Some(new ConsoleProgressBar(sc)))

  def disableProgressBars(sc: SparkContext): Boolean =
    sc.progressBar.isEmpty || trySetProgressBars(sc, None)

  def userAddedClassPath(cl: ClassLoader): Stream[Seq[URL]] =
    if (cl == null) Stream.empty
    else
      cl match {
        case cl: URLClassLoader if cl.getClass.getName == "ammonite.runtime.SpecialClassLoader" =>
          cl.getURLs.toSeq #:: userAddedClassPath(cl.getParent)
        case _ =>
          Stream.empty
      }

  /** Resolves symbolic links in `uri` if it points at a local file
    *
    * So that two URIs pointing at the same file have the same normalized URI.
    */
  private def normalize(uri: URI): URI =
    if (uri.getScheme == "file") {
      val path = Paths.get(uri)
      if (Files.exists(path))
        path.toRealPath().toUri
      else
        uri
    }
    else
      uri
}

class AmmoniteSparkSessionBuilder(implicit
  interpApi: InterpAPI,
  replApi: ReplAPI
) extends SparkSession.Builder {

  override def toString = "AmmoniteSparkSessionBuilder"

  import AmmoniteSparkSessionBuilder.normalize

  def printLine(line: String, htmlLine: String = null): Unit =
    println(line)

  private val options0: scala.collection.Map[String, String] = {

    def optionsField(cls: Class[_]): Option[java.lang.reflect.Field] =
      if (cls == null)
        None
      else
        cls.getDeclaredFields
          .find(f => f.getName == "options" || f.getName.endsWith("$$options"))
          .orElse(optionsField(cls.getSuperclass))

    def optionsViaReflection(): Option[scala.collection.mutable.Map[String, String]] =
      optionsField(classOf[SparkSession.Builder]).map { f =>
        f.setAccessible(true)
        f.get(this).asInstanceOf[scala.collection.mutable.Map[String, String]]
      }

    optionsViaReflection()
      .getOrElse {
        printLine(
          "Warning: can't read SparkSession Builder options (options field not found)",
          "Warning: can't read <code>SparkSession</code> <code>Builder</code> options (<code>options</code> field not found)"
        )
        Map.empty[String, String]
      }
  }

  private def init(): Unit = {

    for {
      envVar <- AmmoniteSparkSessionBuilder.confEnvVars
      path   <- sys.env.get(envVar)
    } {
      printLine(
        s"Loading spark conf from ${AmmoniteSparkSessionBuilder.prettyDir(path)}",
        s"Loading spark conf from <code>${AmmoniteSparkSessionBuilder.prettyDir(path)}</code>"
      )
      loadConf(path)
    }

    config("spark.submit.deployMode", "client")

    for (master0 <- sys.env.get("SPARK_MASTER"))
      master(master0)
  }

  init()

  def loadConf(path: String, ignore: Set[String] = Set()): this.type = {

    val content = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)

    // Same format as spark-defaults.conf: "key value" lines, '#' starting comments
    val extraProps = content
      .linesIterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .map(_.split("\\s+", 2))
      .collect { case Array(k, v) => k -> v }
      .toVector

    for ((k, v) <- extraProps if !ignore(k))
      config(k, v)

    this
  }

  private var forceProgressBars0   = false
  private var disableProgressBars0 = false

  def progressBars(force: Boolean = true): this.type = {
    forceProgressBars0 = force
    this
  }

  def disableProgressBars(disable: Boolean = true): this.type = {
    disableProgressBars0 = disable
    this
  }

  private var sendSparkYarnJars0 = true
  private var sendSparkJars0     = true
  private var ignoreJars0        = Set.empty[URI]
  private var sendSourceJars0    = false
  private var keepJars0          = Seq.empty[URI => Boolean]

  def sendSparkYarnJars(force: Boolean = true): this.type = {
    sendSparkYarnJars0 = force
    this
  }

  def sendSparkJars(force: Boolean = true): this.type = {
    sendSparkJars0 = force
    this
  }

  def ignoreJars(ignore: Set[URI]): this.type = {
    ignoreJars0 = ignoreJars0 ++ ignore
    this
  }

  def sendSourceJars(send: Boolean): this.type = {
    sendSourceJars0 = send
    this
  }

  def keepJars(keep: URI => Boolean): this.type = {
    keepJars0 = keepJars0 :+ keep
    this
  }

  var classServerOpt = Option.empty[AmmoniteClassServer]

  private def isYarn(): Boolean =
    options0.get("spark.master").exists(_.startsWith("yarn"))

  private def hiveSupport(): Boolean =
    options0.get("spark.sql.catalogImplementation").contains("hive")

  private def host(): String =
    options0.get("spark.driver.host")
      .orElse(sys.env.get("HOST"))
      .getOrElse(InetAddress.getLocalHost.getHostAddress)

  private def bindAddress(): String =
    options0.getOrElse("spark.driver.bindAddress", host())

  private def loadExtraDependencies(): Unit = {

    var deps = List.empty[(String, Dependency)]

    if (hiveSupport() && !SparkDependencies.sparkHiveFound())
      deps = ("spark-hive", SparkDependencies.sparkHiveDependency) :: deps

    if (!SparkDependencies.sparkExecutorClassLoaderFound())
      deps = ("spark-stubs", SparkDependencies.stubsDependency) :: deps

    if (isYarn() && !SparkDependencies.sparkYarnFound())
      deps = ("spark-yarn", SparkDependencies.sparkYarnDependency) :: deps

    if (isYarn() && !SparkDependencies.yarnAmIpFilterFound())
      deps = ("hadoop-yarn-server-web-proxy", SparkDependencies.yarnWebProxyDependency) :: deps

    if (deps.nonEmpty) {
      printLine(
        s"Loading ${deps.map(_._1).mkString(", ")}",
        s"Loading ${deps.map("<code>" + _._1 + "</code>").mkString(", ")}"
      )
      interpApi.load.ivy(deps.map(_._2): _*)
    }
  }

  override def getOrCreate(): SparkSession = {

    loadExtraDependencies()

    val (sessionJars, cp) = Option(replApi) match {
      case None =>
        def firstNonSpecialClassLoader(cl: ClassLoader): Option[ClassLoader] =
          if (cl == null)
            None
          else if (cl.getClass.getName == "ammonite.runtime.SpecialClassLoader")
            firstNonSpecialClassLoader(cl.getParent)
          else
            Some(cl)

        val cl = Thread.currentThread().getContextClassLoader

        val sessionJars0 = AmmoniteSparkSessionBuilder.userAddedClassPath(cl).toVector.flatten
          .filter(AmmoniteSparkSessionBuilder.shouldPassToSpark)
          .map(_.toURI)

        val firstNonSpecialClassLoader0 = firstNonSpecialClassLoader(cl).getOrElse {
          sys.error(s"No base class loader found in the hierarchy of $cl")
        }
        val cp = AmmoniteSparkSessionBuilder.classpath(firstNonSpecialClassLoader0)

        (sessionJars0, cp)

      case Some(replApi0) =>
        val sessionJars0 = replApi0
          .sess
          .frames
          .flatMap(_.classpath)
          .filter(AmmoniteSparkSessionBuilder.shouldPassToSpark)
          .map(_.toURI)

        val cp = AmmoniteSparkSessionBuilder.classpath(
          replApi0
            .sess
            .frames
            .last
            .classloader
            .getParent
        )

        (sessionJars0, cp)
    }

    val baseJars = cp
      .map(_.toURI)
      // assuming the JDK on the YARN machines already have those
      .filter(u => !AmmoniteSparkSessionBuilder.isJdkJar(u))
      .toVector

    val jars = (baseJars ++ sessionJars).distinct

    val (sparkJars, sparkDistClassPath) = sys.env.get("SPARK_HOME") match {
      case None =>
        printLine("Getting spark JARs")
        val sparkJars0 =
          SparkDependencies.sparkJars(interpApi.repositories(), interpApi.resolutionHooks, Nil)
        (sparkJars0, Nil)
      case Some(sparkHome) =>
        // Loose attempt at using the scala JARs already loaded in Ammonite,
        // rather than ones from the spark distribution.
        val fromBaseCp = jars.filter { f =>
          f.toASCIIString.contains("/scala-library") ||
          f.toASCIIString.contains("/scala-reflect") ||
          f.toASCIIString.contains("/scala-compiler")
        }
        val fromSparkDistrib = Files.list(Paths.get(sparkHome).resolve("jars"))
          .iterator()
          .asScala
          .toSeq
          .filter { p =>
            val name = p.getFileName.toString
            !name.startsWith("scala-library-") &&
            !name.startsWith("scala-reflect-") &&
            !name.startsWith("scala-compiler-")
          }
          .map(_.toAbsolutePath.toUri)

        val sparkDistClassPath = sys.env.get("SPARK_DIST_CLASSPATH")
          .toList
          .flatMap(ClassPathUtil.classPath(_))

        (fromBaseCp ++ fromSparkDistrib, sparkDistClassPath)
    }

    if (sendSparkYarnJars0 && isYarn())
      config("spark.yarn.jars", sparkJars.map(_.toASCIIString).mkString(","))

    lazy val sparkJarFileSet =
      (sparkJars.iterator ++ sparkDistClassPath.map(_.toUri).iterator ++ ignoreJars0.iterator)
        .map(normalize)
        .toSet

    def toBeSent(jars: Seq[URI]): Seq[URI] = {
      val nonSparkJars = jars.filter(uri => !sparkJarFileSet.contains(normalize(uri)))
      val nonSparkJars0 =
        if (sendSourceJars0) nonSparkJars
        else nonSparkJars.filter(uri => !uri.toASCIIString.endsWith("-sources.jar"))
      keepJars0.foldLeft(nonSparkJars0)(_.filter(_))
    }

    if (sendSparkJars0)
      config("spark.jars", toBeSent(jars).map(_.toASCIIString).mkString(","))

    if (interpApi != null)
      interpApi._compilerManager.outputDir match {
        case None =>
          if (replApi != null) {
            val classServer = new AmmoniteClassServer(
              host(),
              bindAddress(),
              options0.get("spark.repl.class.port").fold(AmmoniteClassServer.randomPort())(_.toInt),
              replApi.sess.frames
            )
            classServerOpt = Some(classServer)

            config("spark.repl.class.uri", classServer.uri.toString)

            System.err.println(
              "Warning: Ammonite output directory not specified upon launch. " +
                "Relying on the spark.repl.class.uri property, which might have issues in tight network environments."
            )
          }

        case Some(outputDir) =>
          config("spark.repl.class.outputDir", outputDir.toAbsolutePath.toString)
      }

    if (!options0.contains("spark.ui.port"))
      config("spark.ui.port", AmmoniteClassServer.availablePortFrom(4040).toString)

    if (isYarn() && !options0.contains("spark.yarn.queue"))
      for (queue <- sys.env.get("SPARK_YARN_QUEUE"))
        config("spark.yarn.queue", queue)

    def coreSiteFound =
      Thread.currentThread().getContextClassLoader.getResource("core-site.xml") != null

    def hiveSiteFound =
      Thread.currentThread().getContextClassLoader.getResource("hive-site.xml") != null

    if (isYarn() && !coreSiteFound) {

      val hadoopConfDirOpt = sys.env.get("HADOOP_CONF_DIR")
        .orElse(sys.env.get("YARN_CONF_DIR"))
        .orElse(Some(new File("/etc/hadoop/conf")).filter(_.isDirectory).map(_.getAbsolutePath))

      hadoopConfDirOpt match {
        case None =>
          printLine(
            "Warning: core-site.xml not found in the classpath, and no hadoop conf found via HADOOP_CONF_DIR, " +
              "YARN_CONF_DIR, or at /etc/hadoop/conf",
            "Warning: <code>core-site.xml</code> not found in the classpath, and no hadoop conf found via <code>HADOOP_CONF_DIR</code>, " +
              "<code>YARN_CONF_DIR</code>, or at <code>/etc/hadoop/conf</code>"
          )
        case Some(dir) =>
          printLine(
            s"Adding Hadoop conf dir ${AmmoniteSparkSessionBuilder.prettyDir(dir)} to classpath",
            s"Adding Hadoop conf dir <code>${AmmoniteSparkSessionBuilder.prettyDir(dir)}</code> to classpath"
          )
          interpApi.load.cp(os.Path(dir))
      }
    }

    if (hiveSupport() && !hiveSiteFound) {

      val hiveConfDirOpt = sys.env.get("HIVE_CONF_DIR")

      hiveConfDirOpt match {
        case None =>
          printLine(
            "Warning: hive-site.xml not found in the classpath, and no Hive conf found via HIVE_CONF_DIR",
            "Warning: <code>hive-site.xml</code> not found in the classpath, and no Hive conf found via <code>HIVE_CONF_DIR</code>"
          )
        case Some(dir) =>
          printLine(
            s"Adding Hive conf dir ${AmmoniteSparkSessionBuilder.prettyDir(dir)} to classpath",
            s"Adding Hive conf dir <code>${AmmoniteSparkSessionBuilder.prettyDir(dir)}</code> to classpath"
          )
          interpApi.load.cp(os.Path(dir))
      }
    }

    if (disableProgressBars0)
      config("spark.ui.showConsoleProgress", false)

    printLine("Creating SparkSession")
    val session = super.getOrCreate()

    if (interpApi != null)
      interpApi.beforeExitHooks += { v =>
        if (!session.sparkContext.isStopped)
          session.sparkContext.stop()
        v
      }

    session.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) =
          for (s <- classServerOpt) {
            s.stop()
            classServerOpt = None
          }
      }
    )

    if (forceProgressBars0)
      AmmoniteSparkSessionBuilder.forceProgressBars(session.sparkContext)
    if (disableProgressBars0)
      AmmoniteSparkSessionBuilder.disableProgressBars(session.sparkContext)

    for (api <- Option(replApi))
      api.sess.frames.head.addHook {
        new ammonite.util.Frame.Hook {
          def addClasspath(additional: Seq[URL]): Unit = {
            val toBeSent0 = toBeSent(additional.map(_.toURI))
            for (uri <- toBeSent0)
              session.sparkContext.addJar(uri.toASCIIString)
          }
        }
      }

    session
  }
}
