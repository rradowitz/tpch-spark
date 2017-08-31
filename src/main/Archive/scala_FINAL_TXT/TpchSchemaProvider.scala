package main.scala

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType} 


/**
 *
 * Defines schemas for tables and reads pipe ("|") separated text files as well as orc, json and parquet into these tables.
 * 
 * Original Author: Savvas Savvides <savvas@purdue.edu>
 * Modified by Raphael Radowitz
 * August 2017 
 */

// TPC-H table schemas
case class Customer(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Order(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Long,
  ps_suppkey: Long,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Supplier(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(spark: SparkSession, inputDir: String) {

  // this is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._
   
  val dfMap = Map(   
    "customer" -> spark.read.textFile(inputDir + "/customer").map(_.split('|')).map(p =>
        Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),
    "lineitem" -> spark.read.textFile(inputDir + "/lineitem").map(_.split('|')).map(p =>
        Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),
    "nation" -> spark.read.textFile(inputDir + "/nation").map(_.split('|')).map(p =>
        Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),
    "region" -> spark.read.textFile(inputDir + "/region").map(_.split('|')).map(p =>
        Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),
    "order" -> spark.read.textFile(inputDir + "/orders").map(_.split('|')).map(p =>
        Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),
    "part" -> spark.read.textFile(inputDir + "/part").map(_.split('|')).map(p =>
        Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),
    "partsupp" -> spark.read.textFile(inputDir + "/partsupp").map(_.split('|')).map(p =>
        Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF(),
    "supplier" -> spark.read.textFile(inputDir + "/supplier").map(_.split('|')).map(p =>
        Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF())
     
  // for implicits
  // need to set column c_custkey nullable = false in order to be able to apply isNull function on it after join operation
  // use Datasets instead of Dataframe
  val customer = setNullableStateOfColumn(dfMap.get("customer").get, "c_custkey", false).as[Customer]
  val lineitem = dfMap.get("lineitem").get.as[Lineitem]
  val nation = dfMap.get("nation").get.as[Nation]
  val region = dfMap.get("region").get.as[Region]
  val order = dfMap.get("order").get.as[Order]
  val part = dfMap.get("part").get.as[Part]
  val partsupp = dfMap.get("partsupp").get.as[Partsupp]
  val supplier = dfMap.get("supplier").get.as[Supplier]

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
  
  private def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = { 
    // get schema  
    val schema = df.schema  
    // modify [[StructField] with name `cn` 
    val newSchema = StructType(schema.map { 
      case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m) 
      case y: StructField => y  
    }) 
    // apply new schema 
    df.sparkSession.createDataFrame( df.rdd, newSchema )
    //df.sparkSession.createDataset( df.rdd, newSchema )    
  }
}
