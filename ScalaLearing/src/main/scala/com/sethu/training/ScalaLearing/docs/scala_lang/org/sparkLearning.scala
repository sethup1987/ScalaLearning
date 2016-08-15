package com.scala.sparkUsecase

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



object sparkLearning {
   def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = ")
  }
  
  def main1(args : Array[String]) {
  val conf = new SparkConf().setAppName("appName").setMaster("localhost");
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
import sqlContext.implicits._


//case class Transaction(Transaction_date:String,Product:String,Price:Int,Payment_Type:String,Name:String,City:String,State:String,Country:String,Account_Created:String,Last_Login:String,Latitude:String,Longitude:String)


val transaction = sc.textFile("/home/reshma/Learning/spark/sampleData3.csv").map(_.split(",")).map(p => Transaction(p(0),p(1).trim.toInt,p(2).trim.toInt,p(3),p(4),p(5),p(6),p(8))).toDF()

//val transaction = sc.textFile("/home/reshma/Learning/Data/SalesJan2009.csv").map(_.split(",")).map(p => Transaction(p(0),p(1),p(2).trim.toInt,p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11))).toDF()
// val tranSmall = sqlContext.sql("SELECT Transaction_date,Product,Price,Payment_Type,Name,City,State,Country FROM tran")

transaction.registerTempTable("tran")
println("transaction count:"+transaction.count())
println("end")
}
  case class Transaction(category:String,Item:Int,UPC:Int,STORE:String,PERIOD:String,SALES_UNITS:String,SALES_VALUE:String,acptr:String)
}