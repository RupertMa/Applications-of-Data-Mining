import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}


object ItemBasedCF{
  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(3)).map(x=>{
      val y=x.split(",")
      (y(0).toInt,y(1).toInt)})
    val data = sc.textFile(args(0)).filter(x=> ! x.contains("userID")).map { x => {
      val y = x.split(",")
      (y(0).toInt, y(1).toInt, y(2).toDouble)
    }}
    val test_set = sc.textFile(args(1)).filter(x=> ! x.contains("userID")).map(x=>{
      val tri= x.split(",")
      (tri(0).toInt,tri(1).toInt,tri(2).toDouble)})
    val training_set=data.subtract(test_set).persist()//可能可以删去
    val product_user=training_set.map(x=>(x._2,(x._1,x._3))).groupByKey().collect().toMap
    val user_product=training_set.map(x=>(x._1,(x._2,x._3))).groupByKey().collect().toMap


    val simi_thres=0.01
    val cal_weights=input.map{case(product1,product2)=>{
      val user_rating1=product_user(product1).toArray
      val user_rating2=product_user(product2).toArray
      val userIDs1=user_rating1.map(_._1)
      val userIDs2=user_rating2.map(_._1)
      val condition=userIDs1.intersect(userIDs2)
      if (condition.length>=2){
        val v1=Array.fill[Double](condition.length)(0)
        val v2=Array.fill[Double](condition.length)(0)
        var i=0
        for ((productID,rating)<-user_rating1.sortBy(_._1)){
          if (condition.contains(productID)) {
            v1(i)=rating
            i=i+1
          }
        }
        i=0
        for ((productID,rating)<-user_rating2.sortBy(_._1)){
          if (condition.contains(productID)) {
            v2(i)=rating
            i=i+1}
        }
        val mean1=v1.sum/v1.length ; val mean2=v2.sum/v2.length
        ((product1,product2),Pearson_Correlation(v1,v2,mean1,mean2))
      }
      else ((Int.MinValue,Int.MinValue),Double.NaN)
    }}.filter(x=>math.abs(x._2)>=simi_thres).collect()

    val weights=cal_weights.toMap
    //println("weights length",weights.size)

    val predictions=test_set.map{case (act_userID,productID,rating)=>{
      val product_rating=user_product(act_userID)
      var buf=List[(Double,Double)]()
      for ((product,rating)<-product_rating){
        try{
          if (product>productID) buf=(rating,weights(productID,product))::buf
          else buf=(rating,weights(product,productID))::buf
        }
        catch{case e:java.util.NoSuchElementException=>{}}
      }
      if (buf.length>=1){
        val P=buf.sortWith(_._2>_._2).take(10).map(x=>{
          val top=x._1*x._2
          val bottom=math.abs(x._2)
          (top,bottom)
        }).reduce((x,y)=>(x._1+y._1,x._2+y._2))
        ((act_userID,productID),(P._1/P._2,1))
      }
      else{
        val ratings=product_rating.map(_._2)
        ((act_userID,productID),(ratings.sum/ratings.size,2))
      }
    }}.collect().toMap


    val output=test_set.map{case(act_userID,productID,rating)=>
      (act_userID,productID,predictions((act_userID,productID)))}.collect()
      .sorted.map(x=>x._1+","+x._2+","+x._3._1).deep.mkString("\n")

    new PrintWriter(args(2)) { write(output); close }

    val intermediate=test_set.map{case(act_userID,productID,rating)=>
      math.abs(predictions((act_userID,productID))._1-rating)}//.persist()
    val abs_diff=intermediate.map{diff=>
      if (diff>=0 && diff<1) ">=0 and <1"
      else if (diff>=1 && diff<2) ">=1 and <2"
      else if (diff>=2 && diff<3) ">=2 and <3"
      else if (diff>=3 && diff<4) ">=3 and <4"
      else ">=4"
    }.countByValue()


    val result=intermediate.map(x=>(x*x,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2))
    //println(result._1,result._2)
    val RMSE=math.sqrt(result._1/result._2)

    println(abs_diff.toArray.sorted.map(x=>x._1+":"+x._2).mkString("\n"))

    println(s"Root Mean Squared Error = $RMSE")

    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")

    val x=predictions.toArray.map(x=>(x._2._2,1)).groupBy(_._1).map(x=>(x._1,x._2.length))
    x.foreach(println)


  }

  def Pearson_Correlation(v1:Array[Double],v2:Array[Double],v1_mean:Double,v2_mean:Double)={
    val numerator=v1.zipWithIndex.
      aggregate(0.0)({case (acc,(value,index))=>acc+(value-v1_mean)*(v2(index)-v2_mean)},{(acc1,acc2)=>acc1+acc2})
    val denominator_1=math.sqrt(v1.aggregate(0.0)({(acc,value)=>acc+math.pow(value-v1_mean,2)},{(acc1,acc2)=>acc1+acc2}))
    val denominator_2=math.sqrt(v2.aggregate(0.0)({(acc,value)=>acc+math.pow(value-v2_mean,2)},{(acc1,acc2)=>acc1+acc2}))
    numerator/(denominator_1*denominator_2)
  }
}
