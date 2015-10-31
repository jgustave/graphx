package com.ms.demo

/**
 * Vertex Attributes.. Typicaly VertexType, and Vertex Id Value
 */
class VertexAttr(val vertexType : String,
                 val vertexValue : String) extends Serializable {


  override def toString = s"VertexAttr(vertexType=$vertexType, vertexValue=$vertexValue)"
}
