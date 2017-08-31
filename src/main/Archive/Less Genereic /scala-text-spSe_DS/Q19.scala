package main.scala

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 19
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q19 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    //this is used to implicitly convert an RDD to a DataFrame.
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    import schemaProvider._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").config("spark.sql.orc.filterPushdown", "true").getOrCreate()
    import spark.implicits._

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#32") &&
          sm($"p_container") &&
          $"l_quantity" >= 7 && $"l_quantity" <= 17 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#35") &&
            md($"p_container") &&
            $"l_quantity" >= 15 && $"l_quantity" <= 25 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#24") &&
              lg($"p_container") &&
              $"l_quantity" >= 26 && $"l_quantity" <= 36 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
  }

}
