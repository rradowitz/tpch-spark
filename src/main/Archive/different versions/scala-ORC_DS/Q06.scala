package main.scala

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 6
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q06 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    // this is used to implicitly convert an RDD to a DataFrame.
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").getOrCreate()
    import spark.implicits._
    import schemaProvider._

    lineitem.filter($"l_shipdate" >= "1993-01-01" && $"l_shipdate" < "1994-01-01" && $"l_discount" >= 0.05 && $"l_discount" < 0.07 && $"l_quantity" < 25)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

}
