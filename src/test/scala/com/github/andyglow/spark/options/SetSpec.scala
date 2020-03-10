package com.github.andyglow.spark.options

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import org.scalatest.{mock => _, _}
import org.scalatest.Matchers._
import org.scalatestplus.mockito.MockitoSugar._
import org.mockito.Mockito.{mock => _, _}

import scala.util._


class SetSpec extends WordSpec {

  def emitOpts[SUBJ, T](r: SUBJ, v: T)(implicit set: Set[SUBJ, T]): Unit = {
    // option
    r.option("opt1", Some(v))
    r.option("opt11", Option.empty[T])

    // either
    r.option("opt2", Right(v))
    r.option("opt22", Left[String, T]("val2"))

    // try
    r.option("opt3", Success(v))
    r.option("opt3", Failure[T](new Exception))
  }

  def doMultiTest[T, R](v: T)(implicit
    set1: Set[DataFrameReader, T],
    set2: Set[DataFrameWriter[R], T],
    set3: Set[DataStreamReader, T],
    set4: Set[DataStreamWriter[R], T]): Unit = {

    "DataFrameReader" in {
      val m1 = mock[DataFrameReader]
      emitOpts(m1, v)
      verify(m1).option("opt1", v)
      verify(m1).option("opt2", v)
      verify(m1).option("opt3", v)

      verifyNoMoreInteractions(m1)
    }

    "DataFrameWriter" in {
      val m1 = mock[DataFrameWriter[R]]
      emitOpts(m1, v)
      verify(m1).option("opt1", v)
      verify(m1).option("opt2", v)
      verify(m1).option("opt3", v)

      verifyNoMoreInteractions(m1)
    }

    "DataStreamReader" in {
      val m1 = mock[DataStreamReader]
      emitOpts(m1, v)
      verify(m1).option("opt1", v)
      verify(m1).option("opt2", v)
      verify(m1).option("opt3", v)

      verifyNoMoreInteractions(m1)
    }

    "DataStreamWriter" in {
      val m1 = mock[DataStreamWriter[R]]
      emitOpts(m1, v)
      verify(m1).option("opt1", v)
      verify(m1).option("opt2", v)
      verify(m1).option("opt3", v)

      verifyNoMoreInteractions(m1)
    }
  }

  "Set" should {
    "support string"  when doMultiTest("value")
    "support long"    when doMultiTest(123L)
    "support boolean" when doMultiTest(false)
    "support double"  when doMultiTest(12.3)
  }
}
