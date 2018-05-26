import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AveTweetLength{
  def main(args: Array[String]): Unit = {
    if (args.length<4){
      System.err.println("Usage: AveTweetLength <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret>"+
        "[<filters>]")
    }

    if (! Logger.getRootLogger.getAllAppenders.hasMoreElements){
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey,consumerSecret,accessToken,accessTokenSecret)=args.take(4)
    val filters=args.takeRight(args.length-4)

    System.setProperty("twitter4j.oauth.consumerKey",consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret",consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret",accessTokenSecret)

    val sparkConf=new SparkConf().setAppName("TwitterPopularTags")

    if (! sparkConf.contains("spark.master")){
      sparkConf.setMaster("local[2]")
    }

    val ssc=new StreamingContext(sparkConf,Seconds(2))
    val stream=TwitterUtils.createStream(ssc,None,filters)

    val hashTags=stream.map(status=>status.getText.toCharArray)

    def combine(a:(Array[Char],Int),b:(Array[Char],Int))=(a._1.union(b._1),a._2+b._2)

    def combine2(a:(Int,Int),b:(Int,Int))=(a._1+b._1,a._2+b._2)


    val lenCounts120=hashTags.map(x=>(x.length,1)).reduceByWindow(combine2,Seconds(120),Seconds(2))//.reduceByWindow(reduceFunc = (x,y)=>x + y , Seconds(120))

    lenCounts120.foreachRDD(rdd=>{
      rdd.foreach{case(len,count)=>
        println("Total tweets: %s, Average length: %3.0f".format( count, len.toDouble / count.toDouble))}
    })



    //lenCounts120.reduceByWindow{ case (x,y)=>(x._1+y._1,x._2+y._2),Seconds(120)}

    //lenCounts120.foreachRDD(rdd=>)


    /*

    lenCounts120.foreachRDD(rdd=>{
      println("Total tweets: %s, Average length: %s".format())
    })





    val topCounts120=hashTags.map(x=>(x,1)).reduceByKeyAndWindow(_+_,Seconds(120))
      .map{case(topic,count)=>(count,topic)}
      .transform(_.sortByKey(false))

    topCounts120.foreachRDD(rdd=>{
      val topList=rdd.take(5)
      println("\nPopular topics in last 120 seconds (%s total)".format(rdd.count()))
      topList.foreach{case(count,tag)=>println("%s (%s tweets)".format(tag,count))}
    })
    */

    ssc.start()
    ssc.awaitTermination()


  }
}
