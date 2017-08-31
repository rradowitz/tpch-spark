package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.LocalDateTime.now
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, className: String): Unit = {
    df.collect().foreach(println)  
  }

  def executeQueries(sc: SparkContext, schemaProvider: TpchSchemaProvider, queryNum: Int): Unit = {

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
      outputDF(query.execute(sc, schemaProvider), query.getName())

    }
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    var scaleFactor = 0;
    if (args.length > 0)
      queryNum = args(0).toInt
      scaleFactor = args(1).toInt
   
    val conf = new SparkConf().setAppName("TPCH on native Spark")
    val sc = new SparkContext(conf)

    // read from hdfs
    val INPUT_DIR: String = "/tmp/tpch-generate/" + scaleFactor.toString

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    executeQueries(sc, schemaProvider, queryNum)
    
  }
}
