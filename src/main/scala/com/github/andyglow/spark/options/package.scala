package com.github.andyglow.spark

import scala.language.implicitConversions


package object options {

  implicit def mkSetAdapter[SUBJ](r: SUBJ): SetAdapter[SUBJ] = new SetAdapter[SUBJ](r)
}
