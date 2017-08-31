package main.scala

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.count

/**
 * TPC-H Query 4
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q04 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
    import sqlContext.implicits._
    import schemaProvider._

    val forders = order.filter($"o_orderdate" >= "1996-05-01" && $"o_orderdate" < "1996-08-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")
  }

}
