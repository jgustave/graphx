package com.ms.demo

/**
 * This is what is extracted from a single line.
 * It implies that the given verticies are connected by the given edge..
 * It is slightly degenerate in that you can have more than 2 verticies, but we are just specifying the single edge that is replicated between them all.
 */
case class DataRow(verticies : Array[VertexAttr], edge : EdgeAttr) {


  override def toString = s"DataRow($verticies, $edge)"
}
