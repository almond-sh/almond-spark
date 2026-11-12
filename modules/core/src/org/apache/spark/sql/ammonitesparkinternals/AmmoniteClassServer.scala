package org.apache.spark.sql.ammonitesparkinternals

import java.io.{File, IOException}
import java.net.{InetSocketAddress, ServerSocket, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import ammonite.util.Frame
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

/** HTTP server serving the classes compiled during an Ammonite session, so that Spark executors can
  * load them (via `spark.repl.class.uri`)
  */
final class AmmoniteClassServer private[ammonitesparkinternals] (
  host: String,
  bindTo: String,
  port: Int,
  classBytes: String => Option[Array[Byte]]
) {

  def this(host: String, bindTo: String, port: Int, frames: => List[Frame]) =
    this(host, bindTo, port, AmmoniteClassServer.classBytes(frames, _))

  private val handler: HttpHandler = { (exchange: HttpExchange) =>
    try {
      val path = exchange.getRequestURI.getPath.stripPrefix("/").stripSuffix(".class")
      val (status, contentType, body) =
        if (exchange.getRequestMethod == "GET" || exchange.getRequestMethod == "HEAD")
          classBytes(path) match {
            case Some(bytes) => (200, "application/octet-stream", bytes)
            case None        => (404, "text/plain", "not found\n".getBytes(StandardCharsets.UTF_8))
          }
        else
          (405, "text/plain", "method not allowed\n".getBytes(StandardCharsets.UTF_8))
      exchange.getResponseHeaders.set("Content-Type", contentType)
      if (exchange.getRequestMethod == "HEAD") {
        exchange.getResponseHeaders.set("Content-Length", body.length.toString)
        exchange.sendResponseHeaders(status, -1L)
      }
      else {
        exchange.sendResponseHeaders(status, body.length.toLong)
        exchange.getResponseBody.write(body)
      }
    }
    finally exchange.close()
  }

  private val executor: ExecutorService = {
    val count = new AtomicInteger
    val threadFactory: ThreadFactory = { (r: Runnable) =>
      val t = new Thread(r, s"ammonite-class-server-${count.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
    Executors.newCachedThreadPool(threadFactory)
  }

  private val server =
    try {
      val server0 = HttpServer.create(new InetSocketAddress(bindTo, port), 0)
      server0.createContext("/", handler)
      server0.setExecutor(executor)
      server0.start()
      server0
    }
    catch {
      case e: Throwable =>
        executor.shutdown()
        throw new Exception(s"Error starting class server at $uri", e)
    }

  def stop(): Unit = {
    server.stop(0)
    executor.shutdown()
  }

  def uri = {
    val sparkBinaryVersion =
      org.apache.spark.SPARK_VERSION
        .split('.')
        .take(2)
        .mkString("")
        .toInt
    val uriStr = if (sparkBinaryVersion >= 32) s"http://$host:$port/" else s"http://$host:$port"
    new URI(uriStr)
  }

}

object AmmoniteClassServer {

  /** Looks for the byte code of a class in an Ammonite session
    *
    * @param path
    *   path of the class, without the `.class` extension, like `ammonite/$sess/cmd2$Helper`
    */
  private[ammonitesparkinternals] def classBytes(
    frames: => List[Frame],
    path: String
  ): Option[Array[Byte]] = {

    val item = path.replace('/', '.')

    def fromClassMaps =
      frames
        .iterator
        .map(_.classloader.inMemoryClasses.get(item))
        .collectFirst { case Some(bytes) => bytes }

    // Don't let requests escape the class directories
    val segments = path.split('/')
    def validPath =
      segments.nonEmpty && segments.forall(s => s.nonEmpty && s != "." && s != "..")

    def fromDirs =
      frames
        .iterator
        .flatMap(_.classpath)
        .filter(_.getProtocol == "file")
        .map(url => new File(url.toURI))
        .filter(_.isDirectory)
        .map(segments.foldLeft(_)(new File(_, _)))
        .collectFirst { case f if f.isFile => Files.readAllBytes(f.toPath) }

    fromClassMaps.orElse(if (validPath) fromDirs else None)
  }

  def randomPort(): Int = {
    val s    = new ServerSocket(0)
    val port = s.getLocalPort
    s.close()
    port
  }

  private def maxPortAttempts = 100

  def availablePortFrom(from: Int): Int =
    Iterator.range(from, math.min(from + maxPortAttempts, 65536))
      .find { port =>
        try {
          new ServerSocket(port).close()
          true
        }
        catch {
          case _: IOException => false
        }
      }
      .getOrElse {
        throw new IOException(
          s"No available port found between $from and ${from + maxPortAttempts - 1}"
        )
      }

}
