//package spark_core
//
//object UdfAggTest {
//
//}
//// 从基类UserDefinedAggregateFunction继承
//class VectorMean64 extends UserDefinedAggregateFunction {
//  // 定义输入的格式
//  // 这个函数将会处理的那一列的数据类型, 因为是64维的向量, 因此是Array[Float]
//  override def inputSchema: org.apache.spark.sql.types.StructType =
//    StructType(StructField("vector", ArrayType(FloatType)) :: Nil)
//
//  // 这个就是上面提到的状态
//  // 在reduce过程中, 需要记录的中间结果. vector_count即为已经统计的向量个数, 而vector_sum即为已经统计的向量的和
//  override def bufferSchema: StructType =
//    StructType(
//      StructField("vector_count", IntegerType) ::
//        StructField("vector_sum", ArrayType(FloatType)) :: Nil)
//
//  // 最终的输出格式
//  // 既然是求平均, 最后当然还是一个向量, 依然是Array[Float]
//  override def dataType: DataType = ArrayType(FloatType)
//
//  override def deterministic: Boolean = true
//
//  // 初始化
//  // buffer的格式即为bufferSchema, 因此buffer(0)就是向量个数, 初始化当然是0, buffer(1)为向量和, 初始化为零向量
//  override def initialize(buffer: MutableAggregationBuffer): Unit = {
//    buffer(0) = 0
//    buffer(1) = Array.fill[Float](64)(0).toSeq
//  }
//
//  // 定义reduce的更新操作: 如何根据一行新数据, 更新一个聚合buffer的中间结果
//  // 一行数据是一个向量, 因此需要将count+1, 然后sum+新向量
//  // addTwoEmb为向量相加的基本实现
//  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    buffer(0) = buffer.getInt(0) + 1
//
//    val inputVector = input.getAs[Seq[Float]](0)
//    buffer(1) = addTwoEmb(buffer.getAs[Seq[Float]](1), inputVector)
//  }
//
//  // 定义reduce的merge操作: 两个buffer结果合并到其中一个bufer上
//  // 两个buffer各自统计的样本个数相加; 两个buffer各自的sum也相加
//  // 注意: 为什么buffer1和buffer2的数据类型不一样?一个是MutableAggregationBuffer, 一个是Row
//  // 因为: 在将所有中间task的结果进行reduce的过程中, 两两合并时是将一个结果合到另外一个上面, 因此一个是mutable的, 它们两者的schema其实是一样的, 都对应bufferSchema
//  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
//    buffer1(1) = addTwoEmb(buffer1.getAs[Seq[Float]](1), buffer2.getAs[Seq[Float]](1))
//  }
//
//  // 最终的结果, 依赖最终的buffer中的数据计算的到, 就是将sum/count
//  override def evaluate(buffer: Row): Any = {
//    val result = buffer.getAs[Seq[Float]](1).toArray
//    val count = buffer.getInt(0)
//    for (i <- result.indices) {
//      result(i) /= (count + 1)
//    }
//    result.toSeq
//  }
//
//  // 向量相加
//  private def addTwoEmb(emb1: Seq[Float], emb2: Seq[Float]): Seq[Float] = {
//    val result = Array.fill[Float](emb1.length)(0)
//    for (i <- emb1.indices) {
//      result(i) = emb1(i) + emb2(i)
//    }
//    result.toSeq
//  }
//  详细解释可以参考上面的代码注释. 核心就是定义四个模块:
//
//    中间结果的格式 --> bufferSchema
//  将一行数据更新到中间结果 buffer 中 --> update
//  将两个中间结果 buffer 合并 --> merge
//  从最后的 buffer 计算需要的结果 --> evaluate
// 注册一下, 使其可以在Spark SQL中使用
//spark.udf.register("avgVector64", new VectorMean64)
//spark.sql("""
//            |select group_id, avgVector64(embedding) as avg_embedding
//            |from embedding_table_name
//            |group by group_id
//""".stripMargin)
//
//// 当然不注册也可以用, 只是不能在Spark SQL中用, 可以直接用来操作DataFrame
//val avgVector64 = new VectorMean64
//val df = spark.sql("select group_id, embedding from embedding_table_name")
//df.groupBy("group_id").agg(avgVector64(col("embedding"))