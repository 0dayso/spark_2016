package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import analyse.Analyse
import org.apache.spark.SparkContext
object Detection {
  def main(args:Array[String]){
    val ana = new Analyse()
    val sparkConf=new SparkConf().setAppName("detection").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(3))
    
    val ips = sc.textFile("ip.txt")
   // val lines = sc.textFile("hdfs://hmaster:9000/xujing/06171511-5").cache()
   // val ips =sc.textFile("hdfs://hmaster:9000/xujing/ip.txt").cache()
    
            
    val goodbad = sc .textFile("good_bad1.txt")    
    val goodbad1 = goodbad.map(_.split("\\s+")).collect()
    val goodbad2 = sc.broadcast(goodbad1)
    
    val lines = ssc.socketTextStream("localhost", 9001, StorageLevel.MEMORY_AND_DISK)

    val ss = lines.map(_.split("\\|",-1)).cache()
    val sss = ss.map { p => ana.goodBad(p,goodbad2) }.filter(p => p(1) !=null)
    
    
    val ip1_ip2_country=ips.map(_.split("\\s+"))
    val localdata=ip1_ip2_country.map{p => (p(0).toLong,p(1).toLong,p(2))}.collect() 
    val localdata1 = sc.broadcast(localdata)
    
    //提取出域名和ttl,ip "0.0.0.0"
    val domain_type_ttl_ip=sss.map{p => (p(0),(p(1),p(2),p(3)))}.reduceByKey((a,b) => (a._1,ana.addTtl(a._2,b._2),ana.addIp(a._3,b._3))).
    map(p => (p._1,p._2._1,p._2._2.split(',').map(p =>ana.changeDouble(p)),p._2._3.split(',').map(a =>ana.changeIp1(a)))).cache()
    println("域名和ttl:*****************************************************************************")
  
    //域名对应 数字比,ttl均值，个数，变化数，标准差,ip数,国家数
    println("域名,类别,(数字比,均值，个数，变化数，标准差,ip数,国家数):**********************************************")
    //val domains7=domain_type_ttl_ip.map{p =>(p._1,p._2,(digit(p._1),average(p._3),numTtl(p._3),change(p._3),deviation(p._3),numIp(p._4),countries(p._4,localdata1)))}.cache()    
    //domains7.take(10).foreach(p=> println("域名和7个属性：	"+p._1+" "+p._2+" "+p._3._1+" "+ p._3._2+" "+ p._3._3+" "+ p._3._4+" "+ p._3._5+" "+ p._3._6+" "+ p._3._7))
    val domains7=domain_type_ttl_ip.map{p =>(p._1+" "+p._2+" "+"1:"+ana.digit(p._1)+" "+"2:"+ana.average(p._3)+" "+"3:"+ana.numTtl(p._3)+" "+"4:"+ana.change(p._3)+" "+"5:"+ana.deviation(p._3)+" "+"6:"+ana.numIp(p._4)+" "+"7:"+ana.countries(p._4,localdata1))}    
    
    domains7.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
}