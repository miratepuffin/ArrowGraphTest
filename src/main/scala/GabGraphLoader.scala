import GraphSchema.{edgeSchema, vertexSchema}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.{BigIntVector, BitVector, VectorSchemaRoot}
import org.apache.commons.lang3.time.StopWatch

import java.io.{BufferedReader, FileReader}
import java.text.SimpleDateFormat

object GabGraphLoader  {
  def apply(filePath:String) = new GabGraphLoader(filePath)
}

class GabGraphLoader(filePath:String) {
  val stopWatch: StopWatch = StopWatch.createStarted
  private val arrowGraph = ArrowGraph()
  stopWatch.stop()
  println(s"Graph Build in: $stopWatch")

  stopWatch.reset()
  stopWatch.start()
  var br = new BufferedReader(new FileReader(filePath))
  br.lines().forEach(line=> parseLine(line))
  stopWatch.stop()
  println(s"Data Ingested in: $stopWatch")


  private def parseLine(tuple:String) = {
    val fileLine = tuple.split(";").map(_.trim)

    val sourceNode = fileLine(2).toInt
    val targetNode = fileLine(5).toInt

    if (targetNode > 0 && targetNode != sourceNode) {
      val creationDate = dateToUnixTime(timestamp = fileLine(0).slice(0, 19))
      arrowGraph.addVertex(creationDate, sourceNode)
      arrowGraph.addVertex(creationDate, targetNode)
      arrowGraph.addEdge(creationDate, sourceNode, targetNode)
    }
  }

  private def dateToUnixTime(timestamp: => String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val dt = sdf.parse(timestamp)
    val epoch = dt.getTime
    epoch
  }

  def getGraph():ArrowGraph = arrowGraph
}

