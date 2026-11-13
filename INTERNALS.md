
## How is the spark classpath handled?

The classpath is entirely managed from Ammonite. You don't need a spark distribution / a `SPARK_HOME`. The Ammonite process corresponds to the Spark driver. The classpath you can access from the session is the one of the driver. Spark is loaded in the driver when the user loads it, e.g. via ``import $ivy.`org.apache.spark::spark-sql:2.3.1` ``. You can load the Spark version of your choice this way.

The executor classpath depends on the cluster manager used:

`AmmoniteSparkSession.builder()` first computes a classpath that consists in the various Spark modules that are loaded in the Ammonnite session (`spark-sql`, `spark-mllib`, etc. if these were loaded byt the user), along with their transitive dependencies. If YARN is used as cluster manager, it adds `spark-yarn` to them and passes this classpath in `"spark.yarn.jars"` in the SparkConf.

`AmmoniteSparkSession.builder()` then computes a second classpath, made of all the JARs that were loaded in the session and those that come from Ammonite itself, and removes from this list the JARs already in the first classpath above (even with non YARN clusters). These JARs are passed to Spark via `"spark.jars"` in the SparkConf.

The code entered by the user during the session is compiled and results in extra classes. These are accessible via a small web server that `AmmoniteSparkSession.builder()` launches, and whose address is passed to Spark via `"spark.repl.class.uri"` in the SparkConf.

## `AmmoniteSparkSession` vs `SparkSession`

See the [README](README.md#ammonitesparksession-vs-sparksession) for what `AmmoniteSparkSession.builder()` does on top of `SparkSession.builder()`.
