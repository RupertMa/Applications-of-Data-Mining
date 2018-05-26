import java.io._
import com.soundcloud.lsh.Lsh
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.HashMap
import scala.math.Ordering.Implicits._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating



object ModelBasedCF{
  def main(args:Array[String]):Unit={
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(0)).filter(x=> ! x.contains("userID")).map(x=>{
      val four=x.split(",")
      (four(0),four(1),four(2))})
    val test_set = sc.textFile(args(1)).filter(x=> ! x.contains("userID")).map(x=>{
      val tri= x.split(",")
      (tri(0),tri(1),tri(2))})
    val training_set=input.subtract(test_set)

    val user_product=training_set.map(x=>(x._1.toInt,x._3.toDouble)).groupByKey().collect().toMap



    val ratings=training_set.map{case (user,item,rate)=>Rating(user.toInt,item.toInt,rate.toDouble)}

    val rank=3
    val numIteration=10
    val model = ALS.train(ratings,rank,numIteration,0.2,1,seed=4)
    val userProducts=test_set.map(x=>(x._1.toInt,x._2.toInt))
    val predictions=model.predict(userProducts).map{case Rating(user,product,rate)=>((user,product),rate)}
    val predictMin=predictions.collect().map(_._2).min
    val predictMax=predictions.collect().map(_._2).max
    val normalize=predictions.map{case((user,product),rating)=>{
      ((user,product),((rating-predictMin)/(predictMax-predictMin))*4+1)}
      }



    val ratesAndPreds=test_set.map(x=>((x._1.toInt,x._2.toInt),x._3.toDouble)).join(normalize)

    val output=ratesAndPreds.map{case ((user,product),(r1,r2))=>{
      (user,product,r2)
    }}.collect().sorted.map(x=>x._1+","+x._2+","+x._3).deep.mkString("\n")

    new PrintWriter(args(2)) { write(output); close }

    val Diff=ratesAndPreds.map{case ((user,product),(r1,r2))=>
    val err= r1 - r2
    math.abs(err)}.persist()

    val abs_diff=Diff.map{diff=>
      if (diff>=0 && diff<1) ">=0 and <1"
      else if (diff>=1 && diff<2) ">=1 and <2"
      else if (diff>=2 && diff<3) ">=2 and <3"
      else if (diff>=3 && diff<4) ">=3 and <4"
      else ">=4"
    }.countByValue()

    println(abs_diff.toArray.sorted.map(x=>x._1+":"+x._2).mkString("\n"))


    val RMSE=math.sqrt(Diff.map(x=>x*x).mean())


    println(s"Root Mean Squared Error = $RMSE")
    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")


  }
}
