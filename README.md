# almond-spark

Run [spark](https://spark.apache.org/) calculations from [Almond](https://github.com/almond-sh/almond)

[![Build Status](https://github.com/almond-sh/almond-spark/actions/workflows/ci.yml/badge.svg)](https://github.com/almond-sh/almond-spark/actions/workflows/ci.yml?query=branch%3Amain)

*almond-spark* allows to create SparkSessions from Almond. It passes some Almond internals to a `SparkSession`, so that spark calculations can be driven from Almond, as one would do from a [spark-shell](https://spark.apache.org/docs/latest/quick-start.html#interactive-analysis-with-the-spark-shell).

The README below is more about the integration with [Ammonite](https://github.com/com-lihaoyi/Ammonite) ([Almond fork](https://github.com/almond-sh/Ammonite)). See the [Almond documentation](https://almond.sh/docs/usage-spark) for the integration in Almond itself.

<img src="ammonite-spark.png" width="800">

## Table of content

1. [Quick start](#quick-start)
2. [`AmmoniteSparkSession` vs `SparkSession`](#ammonitesparksession-vs-sparksession)
   1. [Syncing dependencies](#syncing-dependencies)
3. [Using with standalone cluster](#using-with-standalone-cluster)
4. [Using with YARN cluster](#using-with-yarn-cluster)
5. [Troubleshooting](#troubleshooting)
6. [Compatibility](#compatibility)



## Quick start

Start Ammonite with the `--class-based` option, using Scala 2.12 or 2.13 (the only Scala versions supported for now).
The `--tmp-output-directory` option, available since Ammonite `3.0.0-M0-14-c12b6a59`, is also recommended, especially in "tight" network environments (Kubernetes, …). The [compatibility section](#compatibility) lists the compatible versions of Ammonite and ammonite-spark. Start Ammonite by either following [the Ammonite instructions](https://ammonite.io/#Ammonite-REPL) on its website, then do
```
$ amm --class-based --tmp-output-directory
```
or use [coursier](https://github.com/coursier/coursier),
```
$ cs launch ammonite:3.0.0-M0-60-89836cd8 --scala 2.13.12 -- --class-based --tmp-output-directory
```
or [Scala CLI](https://github.com/VirtusLab/scala-cli)
```
$ scala-cli repl --amm --ammonite-version 3.0.0-M0-60-89836cd8 --scala 2.13.12 -- --class-based --tmp-output-directory
```

At the Ammonite prompt, load the Spark 2.x or 3.x version of your choice, along with ammonite-spark,
```scala
@ import $ivy.`org.apache.spark::spark-sql:3.5.8`
@ import $ivy.`sh.almond::ammonite-spark:0.14.0-RC8`
```
(Note the two `::` before `spark-sql` or `ammonite-spark`, as these are scala dependencies.)

When run from Ammonite, Spark 3.x fails on Java 17 or later unless extra `--add-opens` JVM options are passed (spark-submit adds them itself), so prefer Java 8 or 11 with it (e.g. `cs launch --jvm 11 ammonite:…`). Spark 2.4 requires Scala 2.12.
Support for Spark 4.x (Scala 2.13 and Java 17 only) is in the main branch, but not in a release yet.

Then create a `SparkSession` using the builder provided by *ammonite-spark*
```scala
@ import org.apache.spark.sql._

@ val spark = {
    AmmoniteSparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }
```

Note the use of `AmmoniteSparkSession.builder()`, instead of `SparkSession.builder()` that one would use when e.g. writing a Spark job.

The builder returned by `AmmoniteSparkSession.builder()` extends the one of `SparkSession.builder()`, so that one can call `.appName("foo")`, `.config("key", "value")`, etc. on it.

See below for how to use it with [standalone clusters](#using-with-standalone-cluster), and how to use it with [YARN clusters](#using-with-yarn-cluster).

Note that *ammonite-spark* does *not* rely on a Spark distribution. The driver and executors classpaths are handled from the Ammonite session only, via ``import $ivy.`…` `` statements. See [INTERNALS](INTERNALS.md) for more details.

You can then run Spark calculations, like
```scala
@ def sc = spark.sparkContext // 'def' recommended over 'val', to workaround SparkContext Java serialization issues

@ val rdd = sc.parallelize(1 to 100, 10)

@ val n = rdd.map(_ + 1).sum()
```

## Using with standalone cluster

Simply set the master to `spark://…` when building the session, e.g.
```scala
@ val spark = {
    AmmoniteSparkSession.builder()
      .master("spark://localhost:7077")
      .config("spark.executor.instances", "4")
      .config("spark.executor.memory", "2g")
      .getOrCreate()
  }
```

Ensure the version of Spark used to start the master and executors matches the one loaded in the Ammonite session (via e.g. ``import $ivy.`org.apache.spark::spark-sql:X.Y.Z` ``), and that the machine running Ammonite can access / is accessible from all nodes of the standalone cluster.

## Using with YARN cluster

Set the master to `"yarn"` when building the session, e.g.
```scala
@ val spark = {
    AmmoniteSparkSession.builder()
      .master("yarn")
      .config("spark.executor.instances", "4")
      .config("spark.executor.memory", "2g")
      .getOrCreate()
  }
```

Ensure the configuration directory of the cluster is set in `HADOOP_CONF_DIR` or `YARN_CONF_DIR` in the environment, or is available at `/etc/hadoop/conf`. This directory should contain files like `core-site.xml`, `hdfs-site.xml`, … Ensure also that the machine you run Ammonite on can indeed act as the driver (it should have access to and be accessible from the YARN nodes, etc.).

Before raising issues, ensure you are aware of all that needs to be set up to get a working spark-shell from a Spark distribution, and that all of them are passed in one way or another to the SparkSession created from Ammonite.

## Troubleshooting

### Getting `org.apache.spark.sql.AnalysisException` when calling `.toDS`

Add `org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)` on the same lines as those where you define case classes involved, like
```scala
@ import spark.implicits._
import spark.implicits._

@ org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this); case class Foo(id: String, value: Int)
defined class Foo

@  val ds = List(Foo("Alice", 42), Foo("Bob", 43)).toDS
ds: Dataset[Foo] = [id: string, value: int]
```

(This should likely be added automatically in the future.)

## Compatibility

ammonite-spark relies on the API of Ammonite, which undergoes
non backward compatible changes from time to time. The following table lists
which versions of Ammonite ammonite-spark is built against - so is compatible
with for sure.

| ammonite-spark   | Ammonite | almond |
|------------------|----------|--------|
| `0.1.2`, `0.1.3` | `1.3.2`  |        |
| `0.2.0`          | `1.5.0`  | `0.2.0` |
| `0.3.0`          | `1.6.3`  | `0.3.0` |
| `0.4.0`          | `1.6.5`  | `0.4.0` |
| `0.4.1`          | `1.6.6`  | `0.5.0` |
| `0.4.2`          | `1.6.7`  | `0.5.0` |
| `0.5.0`          | `1.6.9-8-2a27ffe`  | `0.6.0` |
| `0.6.0`, `0.6.1` | `1.6.9-15-6720d42`  | `0.7.0`, `0.8.0` |
| `0.7.0`          | `1.7.1`  | `0.8.1` |
| `0.7.1`          | `1.7.3-3-b95f921`  |         |
| `0.7.2`          | `1.7.4`  | `0.8.2`, `0.8.3` |
| `0.8.0`          | `1.8.1`  |         |
| `0.9.0`          | `2.0.4`  |         |
| `0.10.0`          | `2.1.4`  | `0.10.0` |
| `0.10.1`          | `2.1.4`  | `0.10.1` |
| `0.11.0`          | `2.3.8-36-1cce53f3`  | `0.11.0` |
| `0.12.0`          | `2.3.8-122-9be39deb`  | skipped |
| `0.13.0`          | `2.5.4-8-30448e49` | `0.13.0` |
| `0.13.1`          | `2.5.4-13-1ebd00a6` | `0.13.1` |
| `0.13.2`          | `2.5.4-14-dc4c47bc` | |
| `0.13.3`          | `2.5.4-26-9cd15abe` | |
| `0.13.4`          | `2.5.5-11-626c15b0` | |
| `0.13.5`          | `2.5.5-17-df243e14` | `0.13.2`, `0.13.3` |
| `0.13.6`          | `2.5.6` | `0.13.4`, `0.13.5` |
| `0.13.7`          | `3.0.0-M0-14-c12b6a59` | `0.13.6`, `0.13.7` |
| `0.13.8`          | `3.0.0-M0-17-e7a04255` | `0.13.8` |
| `0.13.9`          | `3.0.0-M0-17-e7a04255` | `0.13.9` to `0.13.14` |
| `0.13.10`, `0.13.11` | `3.0.0-M0-23-f664d7ef` | |
| `0.13.12`         | `3.0.0-M0-23-f664d7ef` | `0.14.0-RC1` to `0.14.0-RC3` |
| `0.14.0-RC8`      | `3.0.0-M0-41-26a93d9c` (\*) | `0.14.0` to `0.14.1-11` |

(\*) Built against a temporary Almond fork of Ammonite (`sh.almond.tmp.ammonite`) rather than
`com.lihaoyi` artifacts. It is known to work with upstream Ammonite `3.0.0-M0-60-89836cd8`, as
used in the [quick start](#quick-start).

The almond column lists the almond versions that depend on each ammonite-spark version.
See the [releases page](https://github.com/almond-sh/almond-spark/releases) for all versions.
