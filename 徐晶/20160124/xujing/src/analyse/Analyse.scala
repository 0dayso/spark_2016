package analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class Analyse extends Serializable{
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
 def changeIp1(s:String):String={
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
  def findCountry(l:Long,localdata: Broadcast[Array[(Long, Long, String)]]):String={
    var s:String="nocountry"
    
    localdata.value.map{p =>
      if(p._1<=l&&l<=p._2){
        s=p._3
      } 
     }
    s
  }

  //输入是ip对应的整数数组和一份国家名单，输出该ip数组中所有ip对应的不同国家数
  def countries(ips: Array[String], localdata: Broadcast[Array[(Long, Long, String)]]): Int = {

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
  
  //输入是域名和善恶域名的名单，返回打好标签的域名
  def goodBad(a:Array[String],br:Broadcast[Array[Array[String]]]):Array[String] ={
    val s =new Array[String](4)
    var result:String = null
    
    br.value.map { p =>
      if (a(0).equals(p(0))) {
        result=p(1)
      }
    }

    s(0)=a(0)
    s(1)=result
    s(2)=a(1)
    s(3)=a(2)
    s  
  }
  
  def addTtl(a:String,b:String):String={
    var c =a
    var d=b
    if(c==""){
      c="0"
    }
    if(d==""){
      d="0"
    }
    c+","+d
  }
  
  def addIp(a:String,b:String):String={
    var c =a
    var d=b
    if(c==""){
      c="0.0.0.0"
    }
    if(d==""){
      d="0.0.0.0"
    }
    c+","+d
  }
}