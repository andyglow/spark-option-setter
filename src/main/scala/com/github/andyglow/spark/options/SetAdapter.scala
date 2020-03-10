package com.github.andyglow.spark.options

class SetAdapter[SUBJ](r: SUBJ) {

  def option[T](k: String, v: T)(implicit set: Set[SUBJ, T]): SUBJ = set(r, k, v)
}
