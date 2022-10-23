package spark_core

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class UdfMappMap extends UserDefinedAggregateFunction {
  //定义输入的格式，交叉特征 只有数值类型 0 1 2 3 4 5 6 7 8 9
  override def inputSchema: StructType =
    StructType(StructField("cross", MapType(IntegerType, LongType)) :: Nil)

  //bufferSchema 就是reduce过程，记录中间结果
  // 现在假设就4个
  //adid isholiday    reduce
  //A      1        =>Map
  //A      0        =>Map(0->1)
  //A      1
  //A      0
  // reduce就是
  override def bufferSchema: StructType =
    StructType(StructField("map_cross", MapType(IntegerType, LongType)) :: Nil)

  // 最终的输出格式化
  override def dataType: DataType = MapType(IntegerType, LongType)

  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    println("1111")
    buffer(0) = Map[Int, Long]()
    println("222")

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputValue = input.getAs[Map[Int, Long]](0)
    println("333")
    buffer(0) = addMap(buffer.getAs[Map[Int, Long]](0), inputValue)
    println("444")

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println("555")
    buffer1(0) = addMap(buffer1.getAs[Map[Int, Long]](0), buffer2.getAs[Map[Int, Long]](0))
    println("666")

  }

  override def evaluate(buffer: Row): Any = {
    println("777")
    val res = buffer.getAs[Map[Int, Long]](0)
    res.foreach(println(_))
    println("888")
    res
  }

  def addMap(Map1: Map[Int, Long], Map2: Map[Int, Long]): Map[Int, Long] = {
    if (Map1 == Nil) {
      Map2
    } else if (Map2 == Nil) {
      Map1
    } else  {
      Map1 ++ Map2.map(t => t._1 -> (t._2 + Map1.getOrElse(t._1, 0L)))
    }


  }
}