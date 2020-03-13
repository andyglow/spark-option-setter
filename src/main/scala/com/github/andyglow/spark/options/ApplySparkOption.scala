package com.github.andyglow.spark.options

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, SaveMode}

import scala.util.{Success, Try}


trait ApplySparkOption[SUBJ, -T] {

  def apply(subj: SUBJ, v: T): SUBJ
}

object ApplySparkOption {

  def mk[SUBJ, T](fn: (SUBJ, T) => SUBJ): ApplySparkOption[SUBJ, T] = new ApplySparkOption[SUBJ, T] {
    override def apply(subj: SUBJ, v: T): SUBJ = fn(subj, v)
  }

  // DataFrameReader
  implicit val dfrs     = mk[DataFrameReader, (String, String)]       { case (r, (k, v)) => r.option(k, v) }
  implicit val dfrb     = mk[DataFrameReader, (String, Boolean)]      { case (r, (k, v)) => r.option(k, v) }
  implicit val dfrl     = mk[DataFrameReader, (String, Long)]         { case (r, (k, v)) => r.option(k, v) }
  implicit val dfrd     = mk[DataFrameReader, (String, Double)]       { case (r, (k, v)) => r.option(k, v) }
  implicit val dfrst    = mk[DataFrameReader, StructType]             { (r, v) => r.schema(v) }

  // DataFrameWriter
  implicit def dfws[T]  = mk[DataFrameWriter[T], (String, String)]    { case (r, (k, v)) => r.option(k, v) }
  implicit def dfwb[T]  = mk[DataFrameWriter[T], (String, Boolean)]   { case (r, (k, v)) => r.option(k, v) }
  implicit def dfwl[T]  = mk[DataFrameWriter[T], (String, Long)]      { case (r, (k, v)) => r.option(k, v) }
  implicit def dfwd[T]  = mk[DataFrameWriter[T], (String, Double)]    { case (r, (k, v)) => r.option(k, v) }
  implicit def dfwsm[T] = mk[DataFrameWriter[T], SaveMode]            { case (r, m) => r.mode(m) }

  // DataStreamReader
  implicit val dsrs     = mk[DataStreamReader, (String, String)]       { case (r, (k, v)) => r.option(k, v) }
  implicit val dsrb     = mk[DataStreamReader, (String, Boolean)]      { case (r, (k, v)) => r.option(k, v) }
  implicit val dsrl     = mk[DataStreamReader, (String, Long)]         { case (r, (k, v)) => r.option(k, v) }
  implicit val dsrd     = mk[DataStreamReader, (String, Double)]       { case (r, (k, v)) => r.option(k, v) }
  implicit val dsrst    = mk[DataStreamReader, StructType]             { (r, v) => r.schema(v) }

  // DataStreamWriter
  implicit def dsws[T]  = mk[DataStreamWriter[T], (String, String)]    { case (r, (k, v)) => r.option(k, v) }
  implicit def dswb[T]  = mk[DataStreamWriter[T], (String, Boolean)]   { case (r, (k, v)) => r.option(k, v) }
  implicit def dswl[T]  = mk[DataStreamWriter[T], (String, Long)]      { case (r, (k, v)) => r.option(k, v) }
  implicit def dswd[T]  = mk[DataStreamWriter[T], (String, Double)]    { case (r, (k, v)) => r.option(k, v) }
  implicit def dswom[T] = mk[DataStreamWriter[T], OutputMode]          { case (r, m) => r.outputMode(m) }
  implicit def dswt[T]  = mk[DataStreamWriter[T], Trigger]             { case (r, t) => r.trigger(t) }

  implicit def setKOpt[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, (String, T)]): ApplySparkOption[SUBJ, (String, Option[T])] = mk[SUBJ, (String, Option[T])] {
    case (r, (k, Some(v))) => set(r, (k, v))
    case (r, (_, None))    => r
  }

  implicit def setOpt[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, T]): ApplySparkOption[SUBJ, Option[T]] = mk[SUBJ, Option[T]] {
    case (r, Some(v)) => set(r, v)
    case (r, None)    => r
  }

  implicit def setKEither[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, (String, T)]): ApplySparkOption[SUBJ, (String, Either[_, T])] = mk[SUBJ, (String, Either[_, T])] {
    case (r, (k, Right(v))) => set(r, (k, v))
    case (r, _)             => r
  }

  implicit def setEither[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, T]): ApplySparkOption[SUBJ, Either[_, T]] = mk[SUBJ, Either[_, T]] {
    case (r, Right(v)) => set(r, v)
    case (r,  _)        => r
  }

  implicit def setKTry[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, (String, T)]): ApplySparkOption[SUBJ, (String, Try[T])] = mk[SUBJ, (String, Try[T])] {
    case (r, (k, Success(v))) => set(r, (k, v))
    case (r, _)          => r
  }

  implicit def setTry[SUBJ, T](implicit
    set: ApplySparkOption[SUBJ, T]): ApplySparkOption[SUBJ, Try[T]] = mk[SUBJ, Try[T]] {
    case (r, Success(v)) => set(r, v)
    case (r, _)          => r
  }
}
