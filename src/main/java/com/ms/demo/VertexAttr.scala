package com.ms.demo

/**
 * Vertex Attributes.. Typicaly VertexType, and Vertex Id Value
 */
case class VertexAttr(vertexType : String,
                      vertexValue : String) extends Serializable {


  override def toString = s"VertexAttr(vertexType=$vertexType, vertexValue=$vertexValue)"


  def canEqual(other: Any): Boolean = other.isInstanceOf[VertexAttr]

  override def equals(other: Any): Boolean = other match {
    case that: VertexAttr =>
      (that canEqual this) &&
      vertexType == that.vertexType &&
      vertexValue == that.vertexValue
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(vertexType, vertexValue)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
