# spark-option-setter

[![Build Status](https://cloud.drone.io/api/badges/andyglow/spark-option-setter/status.svg)](https://cloud.drone.io/andyglow/spark-option-setter)
[![codecov](https://codecov.io/gh/andyglow/spark-option-setter/branch/master/graph/badge.svg?token=Q37E5LWMIK)](https://codecov.io/gh/andyglow/spark-option-setter)
[![mvn](https://img.shields.io/badge/dynamic/json.svg?label=mvn&query=%24.response.docs%5B0%5D.latestVersion&url=https%3A%2F%2Fsearch.maven.org%2Fsolrsearch%2Fselect%3Fq%3Dspark-option-setter_2.12%26start%3D0%26rows%3D1)](https://search.maven.org/artifact/com.github.andyglow/spark-option-setter_2.12/)

> since `0.3.1` it is compiled against scala `2.12` for spark `3.x` only

> earlier versions compiled against both scala `2.11` and `2.12` and aimed to cover spark `2.4.x`

When it comes to specifying options on spark readers and writers sometimes it may look inconvenient
when you need to work with effects like `Option`, `Either`, `Try`.. Although last 2 might mot be considered as a 
good candidate, the first, which is `Option` often is taken into the loop.

And code becomes messy...

```scala
val extra1Option: Option[String] = ???
val extra2Option: Either[String, Long] = ???
val spark: SparkSession = ???

val reader = spark.read.format("parquet").option("foo", "bar")
extra1Option foreach { reader.option("extra-option-1", _) }
extra2Option.right foreach { reader.option("extra-option-2", _) }
val df = reader.load()
```  

So with this simple library you can acheave this level of readiness of code
```scala
import com.github.andyglow.spark.options._

val extra1Option: Option[String] = ???
val extra2Option: Either[String, Long] = ???
val spark: SparkSession = ???

spark.read
  .format("parquet")
  .option("foo", "bar")
  .option("extra-option-1", extra1Option)
  .option("extra-option-2", extra2Option)
  .load()
```    

### Install
```sbt
libraryDependencies += "com.github.andyglow" %% "spark-option-setter" % "0.0.1"
```