package com.github.andyglow.spark.options

import scala.collection.mutable
import scala.reflect.ClassTag


class ApplyOptionAdapter[SUBJ](r: SUBJ) {

  def option[T](k: String, v: T)(implicit apply: ApplySparkOption[SUBJ, (String, T)]): SUBJ = apply(r, (k, v))

  def option[T](v: T)(implicit apply: ApplySparkOption[SUBJ, T]): SUBJ = apply(r, v)

  def options[E[_], T](kv: Map[String, E[T]])(implicit apply: ApplySparkOption[SUBJ, (String, E[T])]): SUBJ =
    kv.foldLeft(r) { case (r, (k, ev)) => r.option(k, ev) }

  def optionMap(implicit clazz: ClassTag[SUBJ], ev: ApplySparkOption[SUBJ, (String, String)]): Map[String, String] = {
    val f = clazz.runtimeClass.getDeclaredField("extraOptions")
    f.setAccessible(true)

    val extraOptions = f.get(r).asInstanceOf[mutable.Map[String, String]]
    extraOptions.toMap
  }
}
