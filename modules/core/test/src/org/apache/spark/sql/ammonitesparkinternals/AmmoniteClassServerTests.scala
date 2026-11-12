package org.apache.spark.sql.ammonitesparkinternals

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, ServerSocket, URL}
import java.nio.charset.StandardCharsets

import utest._

object AmmoniteClassServerTests extends TestSuite {

  private def readAll(is: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf  = new Array[Byte](16384)
    var read = 0
    while ({ read = is.read(buf); read >= 0 })
      baos.write(buf, 0, read)
    baos.toByteArray
  }

  private def request(url: String, method: String = "GET"): (Int, Array[Byte]) = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod(method)
      val code = conn.getResponseCode
      val is   = if (code >= 400) conn.getErrorStream else conn.getInputStream
      val body = if (is == null) Array.emptyByteArray else readAll(is)
      (code, body)
    }
    finally conn.disconnect()
  }

  private def withServer[T](classes: Map[String, Array[Byte]])(f: String => T): T = {
    val port   = AmmoniteClassServer.randomPort()
    val server = new AmmoniteClassServer("localhost", "localhost", port, classes.get(_))
    try f(server.uri.toString.stripSuffix("/"))
    finally server.stop()
  }

  val tests = Tests {

    test("serve classes") {
      val content = "some byte code".getBytes(StandardCharsets.UTF_8)
      withServer(Map("ammonite/$sess/cmd0$Helper" -> content)) { base =>
        val (code, body) = request(s"$base/ammonite/%24sess/cmd0%24Helper.class")
        assert(code == 200)
        assert(body.sameElements(content))
      }
    }

    test("not found") {
      withServer(Map.empty) { base =>
        val (code, _) = request(s"$base/foo/Bar.class")
        assert(code == 404)
      }
    }

    test("only GET and HEAD") {
      val content = "some byte code".getBytes(StandardCharsets.UTF_8)
      withServer(Map("foo/Bar" -> content)) { base =>
        val (headCode, headBody) = request(s"$base/foo/Bar.class", method = "HEAD")
        assert(headCode == 200)
        assert(headBody.isEmpty)
        val (deleteCode, _) = request(s"$base/foo/Bar.class", method = "DELETE")
        assert(deleteCode == 405)
      }
    }

    test("no path traversal") {
      assert(AmmoniteClassServer.classBytes(Nil, "../../etc/passwd").isEmpty)
      assert(AmmoniteClassServer.classBytes(Nil, "foo//Bar").isEmpty)
    }

    test("available port") {
      val socket = new ServerSocket(0)
      try {
        val taken = socket.getLocalPort
        val port  = AmmoniteClassServer.availablePortFrom(taken)
        assert(port != taken)
        assert(port > taken)
      }
      finally socket.close()
    }
  }

}
