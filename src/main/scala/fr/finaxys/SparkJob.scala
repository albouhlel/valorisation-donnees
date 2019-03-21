package fr.finaxys


import org.apache.spark.input.PortableDataStream
import org.apache.tika.metadata._
import org.apache.tika.parser._
import org.apache.tika.sax.WriteOutContentHandler
import java.io._

import fr.finaxys.Extraction



import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark._



object SparkJob{

  def tikaFunc (a: (String, PortableDataStream)) = {

    val file : File = new File(a._1.drop(5))
    val myparser : AutoDetectParser = new AutoDetectParser()
    val stream : InputStream = new FileInputStream(file)
    val handler : WriteOutContentHandler = new WriteOutContentHandler(-1)
    val metadata : Metadata = new Metadata()
    val context : ParseContext = new ParseContext()

    myparser.parse(stream, handler, metadata, context)

    stream.close

    //println(handler.toString())
    //println("------------------------------------------------")
    (handler.toString,a._2,metadata)
  }




  def textRazorFunc(a: (String, PortableDataStream,Metadata)) = {


    (Extraction.extract(a._1,a._3))
    //println(Extraction.extract(a._1))
    //println("------------------------------------------------")
  }









  def main(args: Array[String]) {

    val filesPath = "/home/finaxys/IdeaProjects/projet valorisation donnees/src/main/resources/pdf_files"
    // hdfs:///user/root/Files
    val conf = new SparkConf()
      .setAppName("convert pdf to text")
      .setMaster("local[*]")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .set("es.batch.size.entries","1")


    val sc = new SparkContext(conf)


    val fileData = sc.binaryFiles(filesPath)


    fileData.map(x => tikaFunc(x)).map(x => textRazorFunc(x)).saveJsonToEs("versiontest/json")




  }
}