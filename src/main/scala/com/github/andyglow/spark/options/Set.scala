package com.github.andyglow.spark.options

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

import scala.util.{Success, Try}


trait Set[SUBJ, -T] {

  def apply(subj: SUBJ, k: String, v: T): SUBJ
}

object Set {

  def mk[SUBJ, T](fn: (SUBJ, String, T) => SUBJ): Set[SUBJ, T] = new Set[SUBJ, T] {
    override def apply(subj: SUBJ, k: String, v: T): SUBJ = fn(subj, k, v)
  }

  // DataFrameReader
  implicit val dfrs = mk[DataFrameReader, String]         { (r, k, v) => r.option(k, v) }
  implicit val dfrb = mk[DataFrameReader, Boolean]        { (r, k, v) => r.option(k, v) }
  implicit val dfrl = mk[DataFrameReader, Long]           { (r, k, v) => r.option(k, v) }
  implicit val dfrd = mk[DataFrameReader, Double]         { (r, k, v) => r.option(k, v) }

  // DataFrameWriter
  implicit def dfws[T] = mk[DataFrameWriter[T], String]   { (r, k, v) => r.option(k, v) }
  implicit def dfwb[T] = mk[DataFrameWriter[T], Boolean]  { (r, k, v) => r.option(k, v) }
  implicit def dfwl[T] = mk[DataFrameWriter[T], Long]     { (r, k, v) => r.option(k, v) }
  implicit def dfwd[T] = mk[DataFrameWriter[T], Double]   { (r, k, v) => r.option(k, v) }

  // DataStreamReader
  implicit val dsrs = mk[DataStreamReader, String]        { (r, k, v) => r.option(k, v) }
  implicit val dsrb = mk[DataStreamReader, Boolean]       { (r, k, v) => r.option(k, v) }
  implicit val dsrl = mk[DataStreamReader, Long]          { (r, k, v) => r.option(k, v) }
  implicit val dsrd = mk[DataStreamReader, Double]        { (r, k, v) => r.option(k, v) }

  // DataStreamWriter
  implicit def dsws[T] = mk[DataStreamWriter[T], String]  { (r, k, v) => r.option(k, v) }
  implicit def dswb[T] = mk[DataStreamWriter[T], Boolean] { (r, k, v) => r.option(k, v) }
  implicit def dswl[T] = mk[DataStreamWriter[T], Long]    { (r, k, v) => r.option(k, v) }
  implicit def dswd[T] = mk[DataStreamWriter[T], Double]  { (r, k, v) => r.option(k, v) }

  implicit def setOpt[SUBJ, T](implicit set: Set[SUBJ, T]): Set[SUBJ, Option[T]] = mk[SUBJ, Option[T]] {
    case (r, k, Some(v)) => set(r, k, v)
    case (r, _, None)    => r
  }

  implicit def setEither[SUBJ, T](implicit set: Set[SUBJ, T]): Set[SUBJ, Either[_, T]] = mk[SUBJ, Either[_, T]] {
    case (r, k, Right(v)) => set(r, k, v)
    case (r, _, _)        => r
  }

  implicit def setTry[SUBJ, T](implicit set: Set[SUBJ, T]): Set[SUBJ, Try[T]] = mk[SUBJ, Try[T]] {
    case (r, k, Success(v)) => set(r, k, v)
    case (r, _, _)          => r
  }
}
