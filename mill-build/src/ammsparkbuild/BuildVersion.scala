package ammsparkbuild

object BuildVersion {

  /** The version after `v` if `HEAD` has a `v*` tag, else the version following the latest `v*` tag
    * (`0.13.12` -> `0.13.13-SNAPSHOT`, `0.14.0-RC8` -> `0.14.1-SNAPSHOT`)
    *
    * Falls back to `0.0.1-SNAPSHOT` if no `v*` tag can be found (shallow clones, for example).
    */
  def compute(workspace: os.Path): String = {
    val exactMatch = os.proc("git", "describe", "--exact-match", "--tags", "--match", "v*", "HEAD")
      .call(cwd = workspace, stderr = os.Pipe, check = false)
    if (exactMatch.exitCode == 0) exactMatch.out.trim().stripPrefix("v")
    else {
      val latest = os.proc("git", "describe", "--abbrev=0", "--tags", "--match", "v*", "HEAD")
        .call(cwd = workspace, stderr = os.Pipe, check = false)
      val latestVersion =
        if (latest.exitCode == 0) latest.out.trim().stripPrefix("v")
        else "0.0.0"
      nextSnapshot(latestVersion)
    }
  }

  /** Version of the next release from `version`, like `0.13.12` -> `0.13.13-SNAPSHOT`, or
    * `0.14.0-RC8` -> `0.14.1-SNAPSHOT`
    *
    * Keeps the first 3 components of `version`, and increments the last of them.
    */
  def nextSnapshot(version: String): String = {
    val parts = version.split("[.-]").filter(_.nonEmpty)
    if (parts.length < 3 || !parts.take(3).forall(_.forall(_.isDigit)))
      sys.error(s"Cannot compute the version following $version, expected an X.Y.Z-like version")
    Seq(parts(0), parts(1), (parts(2).toInt + 1).toString).mkString(".") + "-SNAPSHOT"
  }
}
