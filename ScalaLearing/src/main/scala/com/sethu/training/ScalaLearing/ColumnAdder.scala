package com.sethu.training.ScalaLearing

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CoumnAdder extends UserDefinedAggregateFunction {

	def inputSchema: org.apache.spark.sql.types.StructType =
		StructType(StructField("item", IntegerType) :: Nil)

	def bufferSchema: StructType = StructType(
		StructField("item", ArrayType(IntegerType, false)) ::
			StructField("upcno", ArrayType(IntegerType)) :: Nil)

	def dataType: DataType = new StructType().add("item", ArrayType(IntegerType))
		.add("upcno", ArrayType(IntegerType))

	def deterministic: Boolean = true

	def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer(0) = IndexedSeq[Int]()
		buffer(1) = IndexedSeq[Int]()
	}

	def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		val seq1 = buffer(0).asInstanceOf[IndexedSeq[Int]]
		val seq2 = buffer(1).asInstanceOf[IndexedSeq[Int]]
		val item = input.getAs[Int](0)
		val previousItem = seq1.lastOption
		buffer(0) = item +: seq1
		val upcno = if (previousItem.isEmpty) 1 else seq2.lastOption.get + 1
		buffer(1) = upcno +: seq2

	}
	def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		val seqA1 = buffer1(0).asInstanceOf[IndexedSeq[Int]]
		val seqA2 = buffer1(1).asInstanceOf[IndexedSeq[Int]]
		val seqB1 = buffer2(0).asInstanceOf[IndexedSeq[Int]]
		val seqB2 = buffer2(1).asInstanceOf[IndexedSeq[Int]]

		if (!(seqA1.size == 0 || seqB1.size == 0)) {

			//			if (!(seqA1.size == 1 && seqB1.size == 1) &&
			//				((seqA1.head > seqB1.head && seqA1.last > seqB1.head)
			//					|| (seqB1.head > seqA1.head && seqB1.last > seqA1.head))) {
			//				println("first sequence")
			//				seqA1.foreach(println)
			//				println("second sequence")
			//				seqB1.foreach(println)
			//				throw new IllegalArgumentException("data is not completely sorted")
			//			}
			val descending = if (seqA1.last > seqB1.head) true else false
			val newseq = if (descending) seqA1.++:(seqB1) else seqB1.++:(seqA1)
			buffer1(0) = newseq;
		} else {
			println("one of the seq is null #%$^^$^$#############################################################################################")
			println("first sequence")
			seqA1.foreach(println)
			println("second sequence")
			seqB1.foreach(println)
			if (seqA1.size == 0) {
				buffer1(0) = seqB1;
				buffer1(1) = seqB2
			} else if (seqB1.size == 0) {

			}
		}

		//		seqB1.foldLeft(z=>z+:seqA1)
	}

	def evaluate(buffer: Row): Any = {
		buffer
	}
}