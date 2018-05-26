import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}

object TrackHashTags{
  def main(args: Array[String]): Unit = {
    if (args.length<4){
      System.err.println("Usage: TrackHashTags <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret>"+
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

    val hashTags=stream.flatMap(status=>status.getText.split(" ").filter(x=> x.startsWith("#") || x.endsWith("#")))

    val topCounts120=hashTags.map((_,1)).reduceByKeyAndWindow(_+_,Seconds(120))
      .map{case(topic,count)=>(count,topic)}
      .transform(_.sortByKey(false))

    topCounts120.foreachRDD(rdd=>{
      val topList=rdd.take(5)
      println("\nPopular topics in last 120 seconds (%s total)".format(rdd.count()))
      topList.foreach{case(count,tag)=>println("%s, count: %s".format(tag,count))}
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
