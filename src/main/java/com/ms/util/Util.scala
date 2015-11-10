package com.ms.util
import shapeless._
import ops.tuple.FlatMapper
import syntax.std.tuple._

/**
 *
 */
object Util {
  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T] = at[T](Tuple1(_))
  }
  object flatten extends LowPriorityFlatten {
    implicit def caseTuple[P <: Product](implicit fm: FlatMapper[P, flatten.type]) =
      at[P](_.flatMap(flatten))
  }
}
