package com.github.andyglow.spark.options


class ApplyOptionAdapter[SUBJ](r: SUBJ) {

  def option[T](k: String, v: T)(implicit apply: ApplySparkOption[SUBJ, (String, T)]): SUBJ = apply(r, (k, v))

  def option[T](v: T)(implicit apply: ApplySparkOption[SUBJ, T]): SUBJ = apply(r, v)

  def options[E[_], T](kv: Map[String, E[T]])(implicit apply: ApplySparkOption[SUBJ, (String, E[T])]): SUBJ =
    kv.foldLeft(r) { case (r, (k, ev)) => r.option(k, ev) }
}
