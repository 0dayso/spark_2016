package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SampleTest { 
  //输入是Double型的数组，输出是数组平均值
  //用来求TTL均值
  def average(s:Array[Double]):Double={
    var result:Double=0
    var sum:Double=0
    try{
       sum=s.reduceLeft(_ + _)
       val count=s.length
       result=sum/count 
    }catch{
      case _ => result=0
    }
       
    result    
  }
  
  //输入是Double数组，输出是数组中数字改变的次数
  //如数组[4,5,5,6,5]的改变次数是4,因为第一个数是4（此时改变次数是1）。
  //第二个数是5，4到5改变了一次（此时改变次数是2）。第三个数是5，5到5没有改变（此时改变次数依然是2），以此类推
  //用来求TTL值变化次数
  def change(s: Array[Double]): Int = {
    var num:Int=1
    if(s.length==1){
      num=1
    }else{
      for(i <- 1 until s.length){
        if(s(i-1)!=s(i)){
          num = num+1
        }
      }
    }
    num
  }
  
  //输入是double数组，输出是数组的标准差
  //用来求TTL标准差
  def deviation(s:Array[Double]):Double={
    var result:Double=0
    val ave=average(s)
    var sum:Double=0
    for(i <- 0 until s.length){
      sum +=(s(i)-ave)*(s(i)-ave)
    }
    result=Math.sqrt(sum/s.length)
    result
  }
  
  //输入是double数组，输出是数组中不同数的个数
  //用来求TTL值的个数
  def numTtl(s: Array[Double]): Int = {
    var num: Int = 1
    var b=true
    if (s.length == 1) {
      num = 1
    } else {
      for (i <- 0 until s.length-1) {
        val n = s(i+1)
        b=true
        for (j <- 0 until i) {
         if(n==s(j)){
           b=false
         }
        }
        if(b==true){
          num=num+1         
        }
      }
    }
    num
  }
  
  //输入是一个字符串，输出是字符串中数字所中的比例
  //用来求域名中数字百分比
  def digit(s:String):Double={
    val len=s.length()
    var digit:Double=0
    var res:Double=0
    for(i <- 0 to len-1){
      if(s(i)>=48&&s(i)<= 57){
        digit=digit+1
      }
    }
    if(digit!=0){
      res=digit/len
    }
    res
  }
  
  //输入是一个字符串，输出是字符串对应的数
  //比如“123”,对应数字是Double型的数：123。空字符串用0表示
  def changeDouble(s:String):Double={
    var res:Double=0
    if(s==""){
      res=0
    }else{
      try{
        res=s.toDouble
      }catch{
        case _ => res=0
      }
      
    }
    res
  }
  
  //输入是一个字符串表示的域名和该域名对应的一个字符串表示的IP,输出是一个字符串表示的IP
  //如果输入的域名所对应的IP不为空，则直接输出该IP。如果对应的IP为空，则返回的字符串为：“-”+域名
  def changeIp(domain:String,s:String):String={
    var res:String="0"
    if(s==""){
      res="-"+domain
    }else{
      res=s
    }
    res
  }
  
  //输入是String数组，输出是数组中不同字符串的个数
  //用来求域名对应的IP个数  
  def numIp(s:Array[String]):Int={
     var num: Int = 1
     var b=true
     if (s.length == 1) {
      num = 1
    } else {
      for (i <- 0 until s.length-1) {
        val n = s(i+1)
        b=true
        for (j <- 0 until i) {
         if(n.equals(s(j))){
           b=false
         }
        }
        if(b==true){
          num=num+1          
        }
      }
    }
    num
  }
  
  //输入是一个字符串表示的域名和该域名对应的用字符串表示的IP数组，输出是（ip,域名）对，IP数组中每个ip对应一个对 
  def ipDomain(s:(String,Array[String])):Array[(String,String)]={
    var ss=new Array[(String,String)](s._2.length)
    for(i <- 0 until s._2.length){
      ss(i)=(s._2(i),s._1)
    }
    ss    
  }
  
  //输入是一个域名数组，输出是数组中每个域名与该数组的对
  def domainDomains(s:Array[String]):Array[(String,Array[String])]={
    var ss=new Array[(String,Array[String])](s.length)
    for(i <- 0 until s.length){
      ss(i)=(s(i),s)
    }
    ss
  }
 
  //输入是字符串表示的域名和域名对应的ip，输入ip
  //如果ip不为空，直接输出该ip，如果为空，则输出"0.0.0.0"
 def changeIp1(domain:String,s:String):String={
    var res:String="0"
    if(s==""){
      res="0.0.0.0"
    }else{
      res=s
    }
    res
  }
 
 //输入是字符串表示的ip，输出是该ip对应的整数值 
  def ipToLong(s:String):Long={
    val ss=s.split('.')
    var l:Long=0
    l+=ss(0).toLong<<24
    l+=ss(1).toLong<<16
    l+=ss(2).toLong<<8
    l+=ss(3).toLong
    l
  }
  
  //输入是ip对应的整数和一份国家名单，输出该ip对应的国家
  def findCountry(l:Long,localdata: Array[(Long, Long, String)]):String={
    var s:String="nocountry"
    localdata.map{p =>
      if(p._1<=l&&l<=p._2){
        s=p._3
      } 
     }
    s
  }

  //输入是ip对应的整数数组和一份国家名单，输出该ip数组中所有ip对应的不同国家数
  def countries(ips: Array[String], localdata: Array[(Long, Long, String)]): Int = {

    var count: Int = 0

    var l: Long = 0
    var ss = new Array[String](ips.length)

    for (i <- 0 until ips.length) {
      l = ipToLong(ips(i))
      ss(i) = findCountry(l, localdata)
    }

    count = ss.distinct.length

    count

  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("analyse").setMaster("local[4]")
    .set("spark.executor.memory","8g").set("spark.shuffle.spill","false")
   // val conf = new SparkConf().setAppName("analyse").setMaster("spark://192.168.1.103:7077")
    //.set("spark.executor.memory","30g").set("spark.driver.maxResultSize", "30G")
    
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("06171511-5")
    val ips = sc.textFile("ip.txt")
   // val lines = sc.textFile("hdfs://hmaster:9000/xujing/06171511-5").cache()
   // val ips =sc.textFile("hdfs://hmaster:9000/xujing/ip.txt").cache()
    
    val ip1_ip2_country=ips.map(_.split("\\s+")).cache()
    val localdata=ip1_ip2_country.map{p => (p(0).toLong,p(1).toLong,p(2))}.collect
    
    val ss = lines.map(_.split("\\|",-1)).cache()
    println("原始数据：******************************************************************************")
    ss.take(10).foreach(p=>println(p(0)+" "+p(1)+" "+p(2)+" "+p(3)+" "+p(4)+" "+p(5)+" "+p(6)))
    
    //提取出域名和ttl
    val domain_ttl=ss.map{p => (p(3),p(5))}.reduceByKey((a,b) => a + ","+b).map(p => (p._1,p._2.split(',').map(p =>changeDouble(p)))).cache()
    println("域名和ttl:*****************************************************************************")
   domain_ttl.take(10).foreach(p=> println("提取出域名和ttl:	"+p._1+" "+p._2.mkString(" ")))   
    //提取出域名和ip
    val domain_ips=ss.map{p => (p(3),p(6))}.reduceByKey((a,b) => a+ ","+b) .map(p =>(p._1,p._2.split(',').map(a =>changeIp(p._1,a)))).cache()
    println("域名和ip:*******************************************************************************")
   domain_ips.take(10).foreach(p=> println("域名和ip:	"+p._1+" "+p._2.mkString(" ")))
    //提取域名和国家数

        //提取出域名和ip
    val domain_ips1=ss.map{p => (p(3),p(6))}.reduceByKey((a,b) => a+ ","+b) .map(p =>(p._1,p._2.split(',').map(a =>changeIp1(p._1,a)))).cache()
    println("域名和ip1:*******************************************************************************")
    domain_ips1.take(10).foreach(p=> println("域名和ip1:	"+p._1+" "+p._2.mkString(" ")))
    //提取域名和国家数
    val domain_countries=domain_ips1.map{p =>(p._1,countries(p._2,localdata))}.cache()
    println("域名和国家数:******************************************************************************")
    domain_countries.take(10).foreach(p => println("域名和国家数:	"+p._1+ " "+p._2))
    
    //域名对应数字比和ttl4,均值，个数，变化数，标准差
    val domain_digit_ttl4=domain_ttl.map{p =>(p._1,(digit(p._1),average(p._2),numTtl(p._2),change(p._2),deviation(p._2)))}.cache()
    println("域名对应数字比和ttl4,均值，个数，变化数，标准差:******************************************************")
    domain_digit_ttl4.take(10).foreach(p => println("数字比和ttl4:	"+p._1+ "  "+p._2._1+" "+p._2._2+" "+p._2._3+" "+p._2._4+" "+p._2._5)) 
    //域名对应ip数
    val domain_ipnum=domain_ips.map(p => (p._1, numIp(p._2))).cache()   
    println("域名对应ip数:********************************************************************************")
    domain_ipnum.take(10).foreach(p => println(p._1+ " "+p._2))
     
    //域名对应域名数
    val ip_domain=domain_ips.flatMap{p => ipDomain(p)}.cache()
    ip_domain.collect().take(10).foreach(p => println(p._1  + " "+ p._2))
        
    val ipdomains=ip_domain.reduceByKey((a,b) =>a + ","+b).map(p =>(p._2.split(','))).cache()
    val domain_domains=ipdomains.flatMap{p => domainDomains(p)}.reduceByKey((a,b) => a++b).cache()
    val domain_domainCount=domain_domains.map{p => (p._1,p._2.distinct.length)}.cache()
    println("域名对应域名数:******************************************************************************")
    domain_domainCount.take(10).foreach(p => println("域名对应ip数:	"+p._1  + " "+ p._2))
    
    //域名对应域名数和ip数
    val domain_ip2=domain_domainCount.cogroup(domain_ipnum).map{p =>
      val domainCount=p._2._1.toArray
      val ipnum=p._2._2.toArray
      (p._1,(domainCount(0),ipnum(0)))
    }.cache()
      
    //(域名,(域名数,ip数,数字比,均值，个数，变化数，标准差))
    val domains=domain_ip2.cogroup(domain_digit_ttl4)
    val domains7=domains.map{p =>
      val array1=p._2._1.toArray
      val array2=p._2._2.toArray
    
      (p._1,(array1(0)._1,array1(0)._2,array2(0)._1,array2(0)._2,array2(0)._3,array2(0)._4,array2(0)._5)) 
    }.cache()
    
    //(域名,(域名数,ip数,数字比,均值，个数，变化数，标准差,国家数))
    val domains8=domains7.cogroup(domain_countries).map{p =>
      val array1=p._2._1.toArray
      val array2=p._2._2.toArray
    
      (p._1,(array1(0)._1,array1(0)._2,array1(0)._3,array1(0)._4,array1(0)._5,array1(0)._6,array1(0)._7,array2(0))) 
    }.cache()
    
    println("域名,(域名数,ip数,数字比,均值，个数，变化数，标准差,国家数):**********************************************")
    domains8.take(10).foreach(p=> println("域名和8个属性：	"+p._1+" "+p._2._1+" "+ p._2._2+" "+ p._2._3+" "+ p._2._4+" "+ p._2._5+" "+ p._2._6+" "+ p._2._7+" "+ p._2._8))

    }

}