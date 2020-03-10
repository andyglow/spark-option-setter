package com.github.andyglow.spark

import scala.language.implicitConversions


package object options {

  implicit def mkApplyOptionAdapter[SUBJ](r: SUBJ): ApplyOptionAdapter[SUBJ] =
    new ApplyOptionAdapter[SUBJ](r)
}
