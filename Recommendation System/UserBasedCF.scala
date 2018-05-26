import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object UserBasedCF{
  def main(args:Array[String]):Unit={
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(0)).filter(x=> ! x.contains("userID")).map(x=>{
      val four=x.split(",")
      (four(0).toInt,four(1).toInt,four(2).toInt.toDouble)})
    val test_set = sc.textFile(args(1)).filter(x=> ! x.contains("userID")).map(x=>{
      val tri= x.split(",")
      (tri(0).toInt,tri(1).toInt,tri(2).toDouble)})
    val training_set=input.subtract(test_set).persist()
    val training_set_local=training_set.collect()

    val products_length=training_set.map(x=>x._2).distinct().collect().length

    val user_product=training_set.map(x=>(x._1,(x._2,x._3))).groupByKey()
    val product_user=training_set.map(x=>(x._2,(x._1,x._3))).groupByKey()

    val user_product_local=user_product.collect().toMap
    val product_user_local=product_user.collect().toMap



    def lookupWeight(weights:Map[(Int,Int),Double],Key1:Int,Key2:Int)={
      if (Key1>Key2) weights((Key2,Key1))
      else weights((Key1,Key2))
    }

    def lookupUser(trainingSet:Array[(Int,Int,Double)],i:Int)= {
      trainingSet.filter { case (user, product, rating) => product == i}
        .map{case (user, product, rating) => user}.distinct} //RDD里面应该是不能嵌套RDD的

    def lookupProduct(trainingSet:Array[(Int,Int,Double)],i:Int)={
      trainingSet.filter { case (user, product, rating) => user == i}
        .map{case (user, product, rating) => (product,rating)}}


    val act_userVScouser=test_set.map{case (act_userID,productID,rating)=>{
      val coratted_users=product_user_local(productID)
      (act_userID,coratted_users,productID,rating)
    }}.persist()

    val weights_needed=act_userVScouser.flatMap{case(act_userID,co_users,productID,rating)=>{
      val act_rating=user_product_local(act_userID)
      val temp=co_users.map{case(userID,rating)=>{
        ((act_userID,userID),act_rating.toArray,user_product_local(userID).toArray)}}
      temp
      }}.distinct()


    val simi_thres=0.3
    val cal_weights=weights_needed.map{case(user2,product_rating1,product_rating2)=>{
      val productIDs1=product_rating1.map(_._1)
      val productIDs2=product_rating2.map(_._1)
      val condition=productIDs1.intersect(productIDs2)
      if (condition.length>=2){
        val v1=Array.fill[Double](condition.length)(0)
        val v2=Array.fill[Double](condition.length)(0)
        var i=0
        for ((productID,rating)<-product_rating1.sortBy(_._1)){
          if (condition.contains(productID)) {
            v1(i)=rating
            i=i+1
          }
        }
        i=0
        for ((productID,rating)<-product_rating2.sortBy(_._1)){
          if (condition.contains(productID)) {
            v2(i)=rating
            i=i+1}
        }
        (user2,Pearson_Correlation(v1,v2))

      }
      else ((Int.MinValue,Int.MinValue),Double.NaN)
    }}.filter(x=>math.abs(x._2)>=simi_thres).collect()


    val weights=cal_weights.toMap

    val predictions=act_userVScouser.map{case(act_userID,coratted_users,productID,rating)=>{
      var buf=List[(Int,Double)]()
      for ((userID,useRating)<-coratted_users){
        try{
          buf=(userID,weights(act_userID,userID))::buf}
        catch {case e:java.util.NoSuchElementException=>{}}}
      if (buf.length>=5){
      val right=buf.sortWith(_._2>_._2).take(10).map{case (userID,weight)=>{
        val mean=user_product_local(userID).filter(_._1!=productID).map(_._2).sum/(user_product_local(userID).size-1)
        //val mean=(user_product_local(userID).sum-user_product_local(userID)(productID))/(user_product_local(userID).length-1)
        ((user_product_local(userID).filter(_._1==productID).toArray.head._2-mean)*weight,math.abs(weight))
      }}.reduce((x,y)=>( x._1+y._1 ,x._2+y._2))
      val c=right._1 / right._2

      val act_mean= user_product_local(act_userID).map(_._2).sum / user_product_local(act_userID).size
      ((act_userID,productID),act_mean + c)}
      else{
        val act_mean= user_product_local(act_userID).map(_._2).sum / user_product_local(act_userID).size
        ((act_userID,productID),act_mean)
      }

      }}.collect().toMap
    //predictions.take(100).foreach(println)

    val output=act_userVScouser.map{case(act_userID,coratted_users,productID,rating)=>
      (act_userID,productID,predictions((act_userID,productID)))}.collect()
      .sorted.map(x=>x._1+","+x._2+","+x._3).deep.mkString("\n")

    new PrintWriter(args(2)) { write(output); close }

    val intermediate=act_userVScouser.map{case(act_userID,coratted_users,productID,rating)=>
      math.abs(predictions((act_userID,productID))-rating)}//.persist()

    val abs_diff=intermediate.map{diff=>
      if (diff>=0 && diff<1) ">=0 and <1"
      else if (diff>=1 && diff<2) ">=1 and <2"
      else if (diff>=2 && diff<3) ">=2 and <3"
      else if (diff>=3 && diff<4) ">=3 and <4"
      else ">=4"
    }.countByValue()


    val result=intermediate.map(x=>(x*x,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2))
    val RMSE=math.sqrt(result._1/result._2)

    println(abs_diff.toArray.sorted.map(x=>x._1+":"+x._2).mkString("\n"))

    println(s"Root Mean Squared Error = $RMSE")
    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")
  }

  def Pearson_Correlation(v1:Array[Double],v2:Array[Double])={
    val v1_mean=v1.sum/v1.length
    val v2_mean=v2.sum/v2.length
    val numerator=v1.zipWithIndex.
      aggregate(0.0)({case (acc,(value,index))=>acc+(value-v1_mean)*(v2(index)-v2_mean)},{(acc1,acc2)=>acc1+acc2})
    val denominator_1=math.sqrt(v1.aggregate(0.0)({(acc,value)=>acc+math.pow(value-v1_mean,2)},{(acc1,acc2)=>acc1+acc2}))
    val denominator_2=math.sqrt(v2.aggregate(0.0)({(acc,value)=>acc+math.pow(value-v2_mean,2)},{(acc1,acc2)=>acc1+acc2}))
    numerator/(denominator_1*denominator_2)
  }
}
