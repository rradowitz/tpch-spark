package main.scala

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 14
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q14 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    //this is used to implicitly convert an RDD to a DataFrame.
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    import schemaProvider._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").config("spark.sql.orc.filterPushdown", "true").getOrCreate()
    import spark.implicits._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-08-01" && $"l_shipdate" < "1995-09-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))
  }

}
