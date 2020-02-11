package com.andyglow.spark.options

import org.apache.spark.sql.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType


object Setters {

  sealed trait OptionSetterAdapter[S] {
    def set(rw: S, k: String, v: Boolean): S
    def set(rw: S, k: String, v: String): S
    def set(rw: S, k: String, v: Long): S
    def set(rw: S, k: String, v: Double): S
  }

  object OptionSetterAdapter {

    implicit object DataFrameReaderAdapter extends OptionSetterAdapter[DataFrameReader] {
      override def set(rw: DataFrameReader, k: String, v: Boolean): DataFrameReader = rw.option(k, v)
      override def set(rw: DataFrameReader, k: String, v: String): DataFrameReader  = rw.option(k, v)
      override def set(rw: DataFrameReader, k: String, v: Long): DataFrameReader    = rw.option(k, v)
      override def set(rw: DataFrameReader, k: String, v: Double): DataFrameReader  = rw.option(k, v)
    }

    implicit object DataStreamReaderAdapter extends OptionSetterAdapter[DataStreamReader] {
      override def set(rw: DataStreamReader, k: String, v: Boolean): DataStreamReader = rw.option(k, v)
      override def set(rw: DataStreamReader, k: String, v: String): DataStreamReader  = rw.option(k, v)
      override def set(rw: DataStreamReader, k: String, v: Long): DataStreamReader    = rw.option(k, v)
      override def set(rw: DataStreamReader, k: String, v: Double): DataStreamReader  = rw.option(k, v)
    }

    implicit def dataFrameWriterAdapter[T]: OptionSetterAdapter[DataFrameWriter[T]] = new OptionSetterAdapter[DataFrameWriter[T]] {
      override def set(rw: DataFrameWriter[T], k: String, v: Boolean): DataFrameWriter[T] = rw.option(k, v)
      override def set(rw: DataFrameWriter[T], k: String, v: String): DataFrameWriter[T]  = rw.option(k, v)
      override def set(rw: DataFrameWriter[T], k: String, v: Long): DataFrameWriter[T]    = rw.option(k, v)
      override def set(rw: DataFrameWriter[T], k: String, v: Double): DataFrameWriter[T]  = rw.option(k, v)
    }

    implicit def dataStreamWriterAdapter[T]: OptionSetterAdapter[DataStreamWriter[T]] = new OptionSetterAdapter[DataStreamWriter[T]] {
      override def set(rw: DataStreamWriter[T], k: String, v: Boolean): DataStreamWriter[T] = rw.option(k, v)
      override def set(rw: DataStreamWriter[T], k: String, v: String): DataStreamWriter[T]  = rw.option(k, v)
      override def set(rw: DataStreamWriter[T], k: String, v: Long): DataStreamWriter[T]    = rw.option(k, v)
      override def set(rw: DataStreamWriter[T], k: String, v: Double): DataStreamWriter[T]  = rw.option(k, v)
    }
  }

  sealed trait DataOptionSetter[S, V] {

    def set(entity: S, k: String, x: V): S
  }

  trait LowPriorityDataOptionSetters {

    implicit def stringSetter[S](implicit setter: OptionSetterAdapter[S]): DataOptionSetter[S, String] = new DataOptionSetter[S, String] {
      def set(rw: S, k: String, v: String): S = setter.set(rw, k, v)
    }

    implicit def booleanSetter[S](implicit setter: OptionSetterAdapter[S]): DataOptionSetter[S, Boolean] = new DataOptionSetter[S, Boolean] {
      def set(rw: S, k: String, v: Boolean): S = setter.set(rw, k, v)
    }

    implicit def longSetter[S](implicit setter: OptionSetterAdapter[S]): DataOptionSetter[S, Long] = new DataOptionSetter[S, Long] {
      def set(rw: S, k: String, v: Long): S = setter.set(rw, k, v)
    }

    implicit def doubleSetter[S](implicit setter: OptionSetterAdapter[S]): DataOptionSetter[S, Double] = new DataOptionSetter[S, Double] {
      def set(rw: S, k: String, v: Double): S = setter.set(rw, k, v)
    }

    implicit def option[S, V](implicit setter: DataOptionSetter[S, V]): DataOptionSetter[S, Option[V]] =
      new DataOptionSetter[S, Option[V]] {
        def set(r: S, k: String, v: Option[V]): S = v.fold(r)(setter.set(r, k, _))
      }
  }

  object DataOptionSetter extends LowPriorityDataOptionSetters {

    def apply[S, V](implicit setter: DataOptionSetter[S, V]): DataOptionSetter[S, V] = setter
  }

  implicit class SettersDataOptionSetterOps[S](private val s: S) extends AnyVal {

    def option[T](k: String, x: T)(implicit setter: DataOptionSetter[S, T]): S = setter.set(s, k, x)
  }

  implicit class SettersDataFrameReaderOps(private val reader: DataFrameReader) extends AnyVal {

    def schema(x: Option[StructType]): DataFrameReader = x.fold(reader)(reader.schema)

    def scoped[T](fn: DataFrameReader => T): T = fn(reader)
  }

  implicit class SettersDataFrameWriterOps[A](private val reader: DataFrameWriter[A]) extends AnyVal {

    def scoped[T](fn: DataFrameWriter[A] => T): T = fn(reader)
  }

  implicit class SettersDataStreamReaderOps(private val reader: DataStreamReader) extends AnyVal {

    def schema(x: Option[StructType]): DataStreamReader = x.fold(reader)(reader.schema)

    def scoped[T](fn: DataStreamReader => T): T = fn(reader)
  }

  implicit class SettersDataStreamWriterOps[A](private val reader: DataStreamWriter[A]) extends AnyVal {

    def scoped[T](fn: DataStreamWriter[A] => T): T = fn(reader)
  }
}
