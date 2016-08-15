package com.sethu.training.ScalaLearing

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * @author ${user.name}
 */
object App {

	def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)
	type tranType = Transaction

	def main(args: Array[String]) {
		println("Hello World!")
		println("concat arguments = " + foo(args))

		val conf = new SparkConf().setAppName("appName").setMaster("local");
		val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		//val splitRdd=  sc.textFile("/home/reshma/Learning/spark/sampleData3.csv").map(_.split(","))
		//splitRdd.foreach { f => println("count:"+f.length)}
		val transaction: DataFrame = sc.textFile("/home/reshma/Learning/spark/sampleData3.csv").map(_.split(",")).map(p => Transaction(p(0), p(1).trim.toInt, p(2).trim.toInt, p(3), p(4), p(5), p(6), p(7))).toDF()
		transaction.registerTempTable("tran")
		println("transaction count:" + transaction.count())
		println("sorting the data")
		val sortedTran = transaction.sort("category", "Item")
		// sortedTran.groupBy("category").agg($"category").foreach { x => println(x.get(0)) }
		// val groupedData =transaction.groupBy("category")
		println("getting average")
		//		sortedTran.groupBy("category").agg(avg("Item").as("avg")).show()
		val tran1 = transaction.select("category", "Item")
		val tran2 = tran1.dropDuplicates().sort("category", "Item")
		val tran3 = tran2.sort("category", "Item")
		tran1.show(30)
		println("after removing duplicate ######################################################################################################")
		tran2.show(20)
		println("after sortingh data ##########################################################################################################")
		tran3.show(20)
		val adder: CoumnAdder = new CoumnAdder
		val tran4 = tran3.groupBy("category").agg(adder(col("Item")).as("upcno"))
		val dataType: DataType = new StructType().add("item", ArrayType(IntegerType))
			.add("upcno", ArrayType(IntegerType))
		println("iterating upc no ##########################################################################################################")
		tran4.select(col("upcno")).foreach { x => println(x.get(0)) }
		println("after exploding ##########################################################################################################")
		//		tran4.select(tran4.col("category").as("category"), org.apache.spark.sql.functions.explode(tran4.col("upcno")).as("upc")).show(30)

		val result = tran4.explode(col("upcno")) {
			case Row(employee: Seq[Row]) => employee.map(employee =>
				Transaction1(employee(0).asInstanceOf[ArrayType], employee(1).asInstanceOf[ArrayType]))

		}
		result.show()

	}
	case class Transaction1(ItemArray: ArrayType, upcNo: ArrayType)
	case class Transaction(category: String, Item: Int, UPC: Int, STORE: String, PERIOD: String, SALES_UNITS: String, SALES_VALUE: String, acptr: String)

}
