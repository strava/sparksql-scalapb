package com.trueaccord.scalapb.spark

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object ProtoSQL {
  import scala.language.existentials

  val maxSchemaDepth = 30

  def protoToDataFrame[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](
    sparkSession: SparkSession, protoRdd: org.apache.spark.rdd.RDD[T]): DataFrame = {
    sparkSession.createDataFrame(protoRdd.map( x =>
      messageToRow[T](x, 0)
    ), schemaFor[T])
  }

  def protoToDataFrame[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](
    sqlContext: SQLContext, protoRdd: org.apache.spark.rdd.RDD[T]): DataFrame = {
    protoToDataFrame(sqlContext.sparkSession, protoRdd)
  }

  def schemaFor[T <: GeneratedMessage with Message[T]](implicit cmp: GeneratedMessageCompanion[T]) = {
    import org.apache.spark.sql.types._
    import collection.JavaConverters._
    StructType(cmp.javaDescriptor.getFields.asScala.map( x =>
      structFieldFor(x, 0))
    )
  }

  private def toRowData(fd: FieldDescriptor, obj: Any, msgDepth: Integer) = {
    if (msgDepth > maxSchemaDepth) {
      throw new UnsupportedOperationException("Protobufs with schema depth of more than $maxSchemaDepth are not supported.")
    } else {
      fd.getJavaType match {
        case JavaType.BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
        case JavaType.ENUM => obj.asInstanceOf[EnumValueDescriptor].getName
        case JavaType.MESSAGE => messageToRow(obj.asInstanceOf[T forSome {type T <: GeneratedMessage with Message[T]}], msgDepth + 1)
        case _ => obj
      }
    }
  }

  def messageToRow[T <: GeneratedMessage with Message[T]](msg: T, msgDepth: Integer): Row = {
    import collection.JavaConversions._
    Row(
      msg.companion.javaDescriptor.getFields.map {
        fd =>
          val obj = msg.getField(fd)
          if (obj != null) {
            if (fd.isRepeated) {
              obj.asInstanceOf[Traversable[Any]].map(toRowData(fd, _, msgDepth))
            } else {
              toRowData(fd, obj, msgDepth)
            }
          } else null
      }: _*)
  }

  def dataTypeFor(fd: FieldDescriptor, msgDepth: Integer) = {
    if(msgDepth > maxSchemaDepth) {
      StringType
    } else {
      import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
      import org.apache.spark.sql.types._
      fd.getJavaType match {
        case INT => IntegerType
        case LONG => LongType
        case FLOAT => FloatType
        case DOUBLE => DoubleType
        case BOOLEAN => BooleanType
        case STRING => StringType
        case BYTE_STRING => BinaryType
        case ENUM => StringType
        case MESSAGE =>
          import collection.JavaConverters._
          StructType(fd.getMessageType.getFields.asScala.map( x =>
            structFieldFor(x, msgDepth + 1)
          )
        )
      }
    }
  }

  def structFieldFor(fd: FieldDescriptor, msgDepth: Integer): StructField = {
    val dataType = dataTypeFor(fd, msgDepth)
    StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dataType, containsNull = false) else dataType,
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }
}
