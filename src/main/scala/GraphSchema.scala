import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import scala.collection.JavaConverters._
object GraphSchema {

  private val vertexFieldList = List(
    new Field("ID", FieldType.nullable(new ArrowType.Int(64,true)), null),
    new Field("time", FieldType.nullable(new ArrowType.Int(64,true)), null),
    new Field("state", FieldType.nullable(new ArrowType.Bool()), null)
  )

  private val edgeFieldList = List(
    new Field("sourceID", FieldType.nullable(new ArrowType.Int(64,true)), null),
    new Field("destinationID", FieldType.nullable(new ArrowType.Int(64,true)), null),
    new Field("time", FieldType.nullable(new ArrowType.Int(64,true)), null),
    new Field("state", FieldType.nullable(new ArrowType.Bool()), null)
  )

  def vertexSchema():Schema  = {
    new Schema(vertexFieldList.asJava)
  }

  def edgeSchema():Schema  = {
    new Schema(edgeFieldList.asJava)
  }


}
