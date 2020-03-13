package com.github.andyglow.spark.options

import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.scalatest.{mock => _, _}
import org.scalatest.Matchers._
import org.scalatestplus.mockito.MockitoSugar._
import org.mockito.Mockito.{mock => _, _}

import scala.util._


class ApplySparkOptionSpec extends WordSpec {
  import ApplySparkOptionSpec._

  "ApplySparkOptiom" should {

    "support string, string" when {
      val v1 = "opt1" -> "val1"
      val v2 = "opt2" -> "val2"
      val v3 = "opt3" -> "val3"

      "DataFrameReader"  in dataFrameReader.runK(v1, v2, v3)
      "DataFrameWriter"  in dataFrameWriter.runK(v1, v2, v3)
      "DataStreamReader" in dataStreamReader.runK(v1, v2, v3)
      "DataStreamWriter" in dataStreamWriter.runK(v1, v2, v3)
    }

    "support string, boolean" when {
      val v1 = "opt1" -> true
      val v2 = "opt2" -> false
      val v3 = "opt3" -> true

      "DataFrameReader"  in dataFrameReader.runK(v1, v2, v3)
      "DataFrameWriter"  in dataFrameWriter.runK(v1, v2, v3)
      "DataStreamReader" in dataStreamReader.runK(v1, v2, v3)
      "DataStreamWriter" in dataStreamWriter.runK(v1, v2, v3)
    }

    "support string, long" when {
      val v1 = "opt1" -> 0L
      val v2 = "opt2" -> 1L
      val v3 = "opt3" -> 2L

      "DataFrameReader"  in dataFrameReader.runK(v1, v2, v3)
      "DataFrameWriter"  in dataFrameWriter.runK(v1, v2, v3)
      "DataStreamReader" in dataStreamReader.runK(v1, v2, v3)
      "DataStreamWriter" in dataStreamWriter.runK(v1, v2, v3)
    }

    "support string, double" when {
      val v1 = "opt1" -> 1.1
      val v2 = "opt2" -> 2.2
      val v3 = "opt3" -> 3.3

      "DataFrameReader"  in dataFrameReader.runK(v1, v2, v3)
      "DataFrameWriter"  in dataFrameWriter.runK(v1, v2, v3)
      "DataStreamReader" in dataStreamReader.runK(v1, v2, v3)
      "DataStreamWriter" in dataStreamWriter.runK(v1, v2, v3)
    }

    "support StructType" when {
      val v1 = StructType(Seq(StructField("f0", StringType)))
      val v2 = StructType(Seq(StructField("f1", BooleanType)))
      val v3 = StructType(Seq(StructField("f2", LongType)))

      "DataFrameReader"  in dataFrameReader.run(v1, v2, v3)
      "DataStreamReader" in dataStreamReader.run(v1, v2, v3)
    }

    "support SaveMode" when {
      val v1 = SaveMode.Append
      val v2 = SaveMode.ErrorIfExists
      val v3 = SaveMode.Overwrite

      "DataFrameWriter"  in dataFrameWriter.run(v1, v2, v3)
    }

    "support OutputMode" when {
      val v1 = OutputMode.Append
      val v2 = OutputMode.Complete
      val v3 = OutputMode.Update

      "DataStreamWriter"  in dataStreamWriter.run(v1, v2, v3)
    }

    "support Trigger" when {
      val v1 = Trigger.Once
      val v2 = Trigger.ProcessingTime(1000L)
      val v3 = Trigger.Continuous(100L)

      "DataStreamWriter"  in dataStreamWriter.run(v1, v2, v3)
    }
  }
}

object ApplySparkOptionSpec {

  type Opaque[T] = T

  trait DoMock[T] {
    def mk: T
  }

  object DoMock {
    def mk[T](f: => T): DoMock[T] = new DoMock[T] { def mk: T = f }

    def apply[T: DoMock]: T = implicitly[DoMock[T]].mk

    implicit val mockDataFrameReader = mk(mock[DataFrameReader])
    implicit def mockDataFrameWriter[R] = mk(mock[DataFrameWriter[R]])
    implicit val mockDataStreamReader = mk(mock[DataStreamReader])
    implicit def mockDataStreamWriter[R] = mk(mock[DataStreamWriter[R]])
  }

  trait Verify[R, T] {
    def apply(r: R, t: T): Unit
  }

  object Verify {

    def mk[R, T](f: (R, T) => Unit): Verify[R, T] = new Verify[R, T] { def apply(r: R, t: T): Unit = f(r, t) }

    implicit val v0: Verify[DataFrameReader, (String, String)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v1: Verify[DataFrameReader, (String, Boolean)]   = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v2: Verify[DataFrameReader, (String, Long)]      = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v3: Verify[DataFrameReader, (String, Double)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v4: Verify[DataFrameReader, StructType]          = mk { case (rw, v) => rw.schema(v) }

    implicit def v5[R]: Verify[DataFrameWriter[R], (String, String)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v6[R]: Verify[DataFrameWriter[R], (String, Boolean)]   = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v7[R]: Verify[DataFrameWriter[R], (String, Long)]      = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v8[R]: Verify[DataFrameWriter[R], (String, Double)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v9[R]: Verify[DataFrameWriter[R], SaveMode]            = mk { case (rw, v) => rw.mode(v) }

    implicit val v10: Verify[DataStreamReader, (String, String)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v11: Verify[DataStreamReader, (String, Boolean)]   = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v12: Verify[DataStreamReader, (String, Long)]      = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v13: Verify[DataStreamReader, (String, Double)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit val v14: Verify[DataStreamReader, StructType]          = mk { case (rw, v) => rw.schema(v) }

    implicit def v15[R]: Verify[DataStreamWriter[R], (String, String)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v16[R]: Verify[DataStreamWriter[R], (String, Boolean)]   = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v17[R]: Verify[DataStreamWriter[R], (String, Long)]      = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v18[R]: Verify[DataStreamWriter[R], (String, Double)]    = mk { case (rw, (k, v)) => rw.option(k, v) }
    implicit def v19[R]: Verify[DataStreamWriter[R], OutputMode]         = mk { case (rw, v) => rw.outputMode(v) }
    implicit def v20[R]: Verify[DataStreamWriter[R], Trigger]            = mk { case (rw, v) => rw.trigger(v) }
  }

  class OptTest[R <: Object](implicit mock: DoMock[R]) {
    val r = DoMock[R]

    def runK[T](
      t1: (String, T),
      t2: (String, T),
      t3: (String, T))(implicit
      set: ApplySparkOption[R, (String, T)],
      verif: Verify[R, (String, T)]): Unit = {

      // option
      val (k1, v1) = t1
      r.option(k1, Some(v1))
      r.option(k1, Option.empty[T])

      // either
      val (k2, v2) = t2
      r.option(k2, Right(v2))
      r.option(k2, Left[String, T]("err"))

      // try
      val (k3, v3) = t3
      r.option(k3, Success(v3))
      r.option(k3, Failure[T](new Exception))

      verif(verify(r), t1)
      verif(verify(r), t2)
      verif(verify(r), t3)

      verifyNoMoreInteractions(r)
    }

    def run[T](v1: T, v2: T, v3: T)(implicit
      set: ApplySparkOption[R, T],
      verif: Verify[R, T]): Unit = {

      // option
      r.option(Some(v1))
      r.option(Option.empty[T])

      // either
      r.option(Right(v2))
      r.option(Left[String, T]("err"))

      // try
      r.option(Success(v3))
      r.option(Failure[T](new Exception))

      verif(verify(r), v1)
      verif(verify(r), v2)
      verif(verify(r), v3)

      verifyNoMoreInteractions(r)
    }
  }

  val dataFrameReader: OptTest[DataFrameReader] = new OptTest[DataFrameReader]()
  def dataFrameWriter: OptTest[DataFrameWriter[Row]] = new OptTest[DataFrameWriter[Row]]()
  val dataStreamReader: OptTest[DataStreamReader] = new OptTest[DataStreamReader]()
  def dataStreamWriter: OptTest[DataStreamWriter[Row]] = new OptTest[DataStreamWriter[Row]]()
}