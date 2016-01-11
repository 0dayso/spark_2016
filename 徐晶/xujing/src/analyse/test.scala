package analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object test { 

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("analyse").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("good_bad.txt")
    val ss=lines.map(_.split("\\s+")).collect
     
    val sss = sc.broadcast(ss)
    val ssss = sss.value
    
    println(sss.value)
 
   
  }

}