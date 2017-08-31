package main.scala

import org.apache.spark.SparkContext

// TPC-H table schemas

class TpchSchemaProvider(sc: SparkContext, inputDir: String) {

  // this is used to implicitly convert an RDD to a DataFrame.
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //import sqlContext.implicits._
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types.{StructField, StructType} 
  import org.apache.spark.sql.{DataFrame, SQLContext} 
  import org.apache.spark.{SparkConf, SparkContext}
 
  //val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").config("spark.sql.orc.filterPushdown", "true").getOrCreate()
  val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").getOrCreate()
  import spark.implicits._
  
  val dfMap = Map(
    "customer" -> spark.read.orc(inputDir + "/customer").toDF("c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"),

    "lineitem" -> spark.read.orc(inputDir + "/lineitem").toDF("l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"),

    "nation" -> spark.read.orc(inputDir + "/nation").toDF("n_nationkey","n_name","n_regionkey","n_comment"),

    "region" -> spark.read.orc(inputDir + "/region").toDF("r_regionkey","r_name","r_comment"),

    "order" -> spark.read.orc(inputDir + "/orders").toDF("o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"),

    "part" -> spark.read.orc(inputDir + "/part").toDF("p_partkey","p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment"),

    "partsupp" -> spark.read.orc(inputDir + "/partsupp").toDF("ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment"),


    "supplier" -> spark.read.orc(inputDir + "/supplier").toDF("s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"))

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
