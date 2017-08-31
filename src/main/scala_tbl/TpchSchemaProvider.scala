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

// TPC-H table schemas (DataFrame -> Dataset)
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


class TpchSchemaProvider(spark: SparkSession) {

  // this is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._

  // for implicits
  // need to set column c_custkey nullable = false in order to be able to apply isNull function on it after join operation
  // use Datasets instead of Dataframe

  //val customer = dfMap.get("customer").get.as[Customer]
  val customer = setNullableStateOfColumn(spark.sql("select * from customer"), "c_custkey", false).as[Customer]
  val lineitem = spark.sql("select * from lineitem").as[Lineitem]
  val nation = spark.sql("select * from nation").as[Nation]
  val region =spark.sql("select * from region").as[Region]
  val order = spark.sql("select * from orders").as[Order]
  val part = spark.sql("select * from part").as[Part]
  val partsupp = spark.sql("select * from partsupp").as[Partsupp]
  val supplier = spark.sql("select * from supplier").as[Supplier]
  
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
