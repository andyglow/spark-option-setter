package com.github.andyglow.spark.options


class ApplyOptionAdapter[SUBJ](r: SUBJ) {

  def option[T](k: String, v: T)(implicit apply: ApplySparkOption[SUBJ, T]): SUBJ = apply(r, k, v)
}
