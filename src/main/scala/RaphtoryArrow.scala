import org.apache.arrow.vector.BigIntVector

object RaphtoryArrow extends App {

  val graph = GabGraphLoader("data/gab500.csv").getGraph()
  println(graph.edgeSchemaRoot.getVector("sourceID").asInstanceOf[BigIntVector].get(55))

}
