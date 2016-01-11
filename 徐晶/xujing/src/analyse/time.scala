package analyse

import scala.io.Source
import java.io.PrintWriter

object time {
  def main(args:Array[String]){
    val out = new PrintWriter("06171511-5")
    val source=Source.fromFile("06171511")
    val lines=source.getLines()
    
    for(l <- lines){
      val s = l.split(":")(1)
      if(s=="52"){
        println(l)
        out.println(l)
      }
      
    }
    
    out.close
    
  }

}