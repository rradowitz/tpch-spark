package main.scala

import org.apache.spark.SparkContext

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

class TpchSchemaProvider(sc: SparkContext, inputDir: String) {
  
  import org.apache.spark.sql.types.{StructField, StructType} 
  import org.apache.spark.sql.{DataFrame, SQLContext} 
  import org.apache.spark.{SparkConf, SparkContext}
  // this is used to implicitly convert an RDD to a DataFrame.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
  import sqlContext.implicits._
  
  val dfMap = Map(
    "customer" -> sqlContext.read.orc(inputDir + "/customer").map(p =>
      Customer(p(0).toString.toLong, p(1).toString.trim, p(2).toString.trim, p(3).toString.toInt, p(4).toString.trim, p(5).toString.toDouble, p(6).toString.trim, p(7).toString.trim)).toDF(),

    "lineitem" -> sqlContext.read.orc(inputDir + "/lineitem").map(p =>
      Lineitem(p(0).toString.toLong, p(1).toString.toLong, p(2).toString.toLong, p(3).toString.toLong, p(4).toString.toDouble, p(5).toString.toDouble, p(6).toString.toDouble, p(7).toString.toDouble, p(8).toString.trim, p(9).toString.trim, p(10).toString.trim, p(11).toString.trim, p(12).toString.trim, p(13).toString.trim, p(14).toString.trim, p(15).toString.trim)).toDF(),

    "nation" -> sqlContext.read.orc(inputDir + "/nation").map(p =>
      Nation(p(0).toString.toInt, p(1).toString.trim, p(2).toString.toInt, p(3).toString.trim)).toDF(),

    "region" -> sqlContext.read.orc(inputDir + "/region").map(p =>
      Region(p(0).toString.toInt, p(1).toString.trim, p(2).toString.trim)).toDF(),

    "order" -> sqlContext.read.orc(inputDir + "/orders").map(p =>
      Order(p(0).toString.toLong, p(1).toString.toLong, p(2).toString.trim, p(3).toString.toDouble, p(4).toString.trim, p(5).toString.trim, p(6).toString.trim, p(7).toString.toInt, p(8).toString.trim)).toDF(),

    "part" -> sqlContext.read.orc(inputDir + "/part").map(p =>
      Part(p(0).toString.toLong, p(1).toString.trim, p(2).toString.trim, p(3).toString.trim, p(4).toString.trim, p(5).toString.toInt, p(6).toString.trim, p(7).toString.toDouble, p(8).toString.trim)).toDF(),

    "partsupp" -> sqlContext.read.orc(inputDir + "/partsupp").map(p =>
      Partsupp(p(0).toString.toLong, p(1).toString.toLong, p(2).toString.toInt, p(3).toString.toDouble, p(4).toString.trim)).toDF(),

    "supplier" -> sqlContext.read.orc(inputDir + "/supplier").map(p =>
      Supplier(p(0).toString.toLong, p(1).toString.trim, p(2).toString.trim, p(3).toString.toInt, p(4).toString.trim, p(5).toString.toDouble, p(6).toString.trim)).toDF())

  // for implicits
  // val customer = dfMap.get("customer").get
  // need to set column c_custkey nullable = false in order to be able to apply isNull function on it after join operation
  val customer = setNullableStateOfColumn(dfMap.get("customer").get, "c_custkey", false)
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
  
  // to set DS customer id nullable false to be able to check isNull in Q22
  private def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = { 
    // get schema  
    val schema = df.schema  
    // modify [[StructField] with name `cn` 
    val newSchema = StructType(schema.map { 
      case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m) 
      case y: StructField => y  
    }) 
    // apply new schema 
    df.sqlContext.createDataFrame( df.rdd, newSchema )  
  }

}
