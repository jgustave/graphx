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


  /**
   * Given an Array of N items, return below the diagonal. (Half the cartesian, with no references to yourself
   * @param input
   * @return
   */
//  def diagonal(input : Array ) : Array = {
//    for (i <- 0 to input.length - 2; j <- i + 1 until input.length)
//      yield ( Array(input(i), input(j)) )
//  }

//  def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
//    case Nil => List(Nil)
//    case h : T => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
//  }



  //def halfcross[Y](xs: Traversable[Y], ys: Traversable[Y]) = for { x <- xs; y <- ys  if(x <= y)} yield (x, y)


}
