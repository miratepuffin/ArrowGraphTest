import GraphSchema.{edgeSchema, vertexSchema}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, VectorSchemaRoot}
import org.apache.arrow.algorithm.sort.IndexSorter
object ArrowGraph {
  def apply():ArrowGraph = new ArrowGraph
}

class ArrowGraph {

  val allocator = new RootAllocator
  val vertexSchemaRoot: VectorSchemaRoot = VectorSchemaRoot.create(vertexSchema(), allocator)
  val edgeSchemaRoot: VectorSchemaRoot = VectorSchemaRoot.create(edgeSchema(), allocator)
  var vertexCount =0
  var edgeCount = 0

  def addVertex(time: Long, ID: Int):Unit = {
    vertexSchemaRoot.getVector("ID").asInstanceOf[BigIntVector].setSafe(vertexCount,time)
    vertexSchemaRoot.getVector("time").asInstanceOf[BigIntVector].setSafe(vertexCount,time)
    vertexSchemaRoot.getVector("state").asInstanceOf[BitVector].setSafe(vertexCount,1)
    vertexCount+=1
  }

  def addEdge(time: Long, source: Int, destination: Int) = {
    edgeSchemaRoot.getVector("sourceID").asInstanceOf[BigIntVector].setSafe(edgeCount,source)
    edgeSchemaRoot.getVector("destinationID").asInstanceOf[BigIntVector].setSafe(edgeCount,destination)
    edgeSchemaRoot.getVector("time").asInstanceOf[BigIntVector].setSafe(edgeCount,time)
    edgeSchemaRoot.getVector("state").asInstanceOf[BitVector].setSafe(edgeCount,1)
    edgeCount+=1
  }

}
