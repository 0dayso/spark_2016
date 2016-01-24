package test

import scala.io.Source
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer

object GoodBad {
  def main(args:Array[String]){
    val out = new PrintWriter("result.txt")
    val source=Source.fromFile("06171511-5")
    val lines=source.getLines()
    val array =ArrayBuffer[String]()
    
    for(l <- lines){
      val s = l.split("\\|",-1)(3)

      array += s        
    }   
    val ss=array.distinct
    for(a <- ss){
      out.println(a)

    }
    out.close  
  }
}