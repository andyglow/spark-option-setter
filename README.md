# spark-option-setter

[![Build Status](https://travis-ci.org/andyglow/spark-option-setter.svg?branch=master)](https://travis-ci.org/andyglow/spark-option-setter)
[![Coverage Status](https://coveralls.io/repos/github/andyglow/spark-option-setter/badge.svg?branch=master)](https://coveralls.io/github/andyglow/spark-option-setter?branch=master)
![Maven Central](https://img.shields.io/maven-central/v/com.github.andyglow/spark-option-setter_2.11?color=%23aabb)
![Maven Central](https://img.shields.io/maven-central/v/com.github.andyglow/spark-option-setter_2.12?color=%23aabb)

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