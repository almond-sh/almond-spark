package ammsparkbuild

import mill.scalalib.*

object Deps {
  def almondToreeHooks = mvn"sh.almond::toree-hooks:${Versions.almond}"
  def ammoniteCompiler = mvn"sh.almond.ammonite:::ammonite-compiler:${Versions.ammonite}"
  def ammoniteReplApi  = mvn"sh.almond.ammonite:::ammonite-repl-api:${Versions.ammonite}"
  def ammoniteRepl     = mvn"sh.almond.ammonite:::ammonite-repl:${Versions.ammonite}"

  def classPathUtil = mvn"io.get-coursier::class-path-util:0.1.4"
  def jettyServer   = mvn"org.eclipse.jetty:jetty-server:12.1.13"
  def jsoniterScalaCore =
    mvn"com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-core:${Versions.jsoniterScala}"
  def jsoniterScalaMacros =
    mvn"com.github.plokhotnyuk.jsoniter-scala::jsoniter-scala-macros:${Versions.jsoniterScala}"
  def log4j2       = mvn"org.apache.logging.log4j:log4j-core:2.17.2"
  def slf4jLog4j12 = mvn"org.slf4j:slf4j-log4j12:1.7.36"
  def log4jCore(version: String) =
    mvn"org.apache.logging.log4j:log4j-core:$version"
  def log4jSlf4jImpl(version: String) =
    mvn"org.apache.logging.log4j:log4j-slf4j-impl:$version"
  def log4jSlf4j2Impl(version: String) =
    mvn"org.apache.logging.log4j:log4j-slf4j2-impl:$version"
  def slf4jApi(version: String) = mvn"org.slf4j:slf4j-api:$version"
  def scalaKernelApi            = mvn"sh.almond:::scala-kernel-api:${Versions.almond}"
  def scalatags                 = mvn"com.lihaoyi::scalatags:0.13.1"
  def sparkSql(sv: String) = {
    val ver =
      if (sv.startsWith("2.12.")) "2.4.0"
      else "3.2.0"
    mvn"org.apache.spark::spark-sql:$ver"
  }
  def utest = mvn"com.lihaoyi::utest:0.9.5"
}
