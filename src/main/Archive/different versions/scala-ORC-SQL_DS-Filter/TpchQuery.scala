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

  def outputDF(df: DataFrame, outputDir: String, className: String, outputformat: String, sprint: Int): Unit = {

    if (sprint == 1)
      df.collect().foreach(println)	
     
    if (outputformat != "off")
      if (outputDir != null || outputDir != "")
        if (outputformat == "orc" || outputformat == "json" || outputformat == "csv" || outputformat == "parquet")
      	  df.write.mode("overwrite").format(outputformat).option("header", "true").save(outputDir + "/" + className + ".out")
          //df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className) 
          //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias  
  }

  def executeQueries(sc: SparkContext, schemaProvider: TpchSchemaProvider, scaleFactor: Int, benchNum: Int ,queryNum: Int, outputdir: String, outputformat: String, sprint: Int): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    // val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/dbgen/output"
    //val OUTPUT_DIR: String = "/native_spark/output/" + benchNum.toString
    val OUTPUT_DIR: String =  outputdir + outputformat + "/" + benchNum.toString

    val results = new ListBuffer[(String, Float)]
	
    var fromNum = 1
    var toNum = 22
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName(), outputformat, sprint)

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  def main(args: Array[String]): Unit = {

    var scaleFactor = 0
    var benchNum = 0
    var queryNum = 0
    var inputdir = "" 
    var outputdir = ""
    var outputformat = ""
    var sprint = 1
    var nFormat = ""
    var nAPI = ""
    if (args.length > 0)
      scaleFactor = args(0).toInt
      benchNum = args(1).toInt
      queryNum = args(2).toInt
      inputdir = args(3).toString 
      outputdir = args(4).toString
      outputformat = args(5).toString
      sprint = args(6).toInt
      nFormat = args(7).toString
      nAPI = args(8).toString
   
    val conf = new SparkConf().setAppName("TPCH on native Spark")
    val sc = new SparkContext(conf)

    // read files from local FS
    // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

    // read from hdfs
    //val INPUT_DIR: String = "/tmp/tpch-generate/" + scaleFactor.toString
    //val INPUT_DIR: String = "/apps/hive/warehouse/tpch_orc_" + scaleFactor.toString + ".db"
    val INPUT_DIR: String = inputdir

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, schemaProvider, scaleFactor, benchNum, queryNum, outputdir, outputformat, sprint)

    val outFile = new File("Native_Spark_TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      //case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\t" + scaleFactor.toString + "\t" + now().toString + "\t" + benchNum.toString + "\t" + nFormat + "\t" + nAPI + "\n")
    }

    bw.close()
  }
}
