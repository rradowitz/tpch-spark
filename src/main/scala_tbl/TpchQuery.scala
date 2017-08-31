package main.scala

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

/**
 * Parent class for TPC-H queries.
 *
 * For Execution of TPC-H Queries
 *
 * Original Author: Savvas Savvides <savvas@purdue.edu>
 * Modified by Raphael Radowitz
 * August 2017 
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
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String, outFormat: String, sprint: Int): Unit = {

    if (sprint == 1)
      df.collect().foreach(println)	
    else 
      if (!outputDir.nonEmpty || outputDir != null)
        if (outFormat.equals("orc") || outFormat.equals("json") || outFormat.equals("csv") || outFormat.equals("parquet"))
      	  df.write.mode("overwrite").format(outFormat).option("header", "true").save(outputDir + "/" + className + ".out")
          //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias  
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, benchNum: Int ,queryNum: Int, outputdir: String, outFormat: String, sprint: Int): Unit = {

    val OUTPUT_DIR: String =  outputdir + outFormat + "/" + benchNum.toString

    var fromNum = 1
    var toNum = 22
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(spark, schemaProvider), OUTPUT_DIR, query.getName(), outFormat, sprint)
    }
  }

  def main(args: Array[String]): Unit = {

    var benchNum = 0
    var queryNum = 0
    var outputdir = ""
    var outFormat = ""
    var sprint = 1
    var filter = 0
    var db = "tpch_orc_1"
    if (args.length > 0) {
      benchNum = args(0).toInt
      queryNum = args(1).toInt
      outputdir = args(2).toString
      outFormat = args(3).toString
      sprint = args(4).toInt
      filter = args(5).toInt
      db = args(6).toString
    } 
     
    // Create Spark session
    val spark = SparkSession.builder().appName("TPC-H on Native Spark Session").enableHiveSupport().getOrCreate() 
    //spark.sql("use "+db)
    spark.catalog.setCurrentDatabase(db)
    
    val schemaProvider = new TpchSchemaProvider(spark)

    // Call function for Query execution if format is supported
    // Set filter
    if (filter == 1) {
      spark.conf.set("spark.sql.orc.filterPushdown", "true")
      println("FilterPushDown --> ON")
    }    

    executeQueries(spark, schemaProvider, benchNum, queryNum, outputdir, outFormat, sprint)
  }
}