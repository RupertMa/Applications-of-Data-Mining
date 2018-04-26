import java.io._

import org.apache.spark.{SparkConf, SparkContext}

object SON{
  def main(args:Array[String]):Unit= {
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(1))
    val data=input.filter(x=> ! x.contains("reviewerID")) //change this later
    var r=data.map(x=>{
      if (args(0)=="1") {
        val y=x.split(",")
        (y(0),y(1))}
      else if (args(0)=="2") {
        val y=x.split(",")
        (y(1),y(0))
      }
      else
        ("0","0")
    }).groupByKey().map(x=>x._2.toSet).persist()


    if (args(2).toFloat/r.getNumPartitions <= 5) {r=r.repartition(1).persist()}
    val PartitionNum=r.getNumPartitions

    println("===================Partions:===============",PartitionNum)
    val candidates=r.mapPartitions(x=>{
      A_Priori(x,args(2).toFloat/PartitionNum)
    }).distinct().collect()

    val result=r.flatMap(x=>
    {
      var buf=Seq(scala.collection.SortedSet("")).drop(1)
      for (candidate <- candidates)
        if (candidate.toSet.subsetOf(x)){
          buf = buf :+ candidate}
      buf
    }).countByValue().filter(x=>x._2>=args(2).toFloat).keys

    println("Num:",result.size)
    val size_item=result.groupBy(_.size)
    var res=""
    for (i<-size_item.keys.toList.sorted){
      res=res+sort(size_item(i).toList).map(x=>"('"+x.mkString("','")+"')").mkString(",")+"\n\n"
    }
    val filename="SON_"+args(1).substring(args(1).lastIndexOf("/")+1,args(1).length-4)+
      ".case"+args(0)+"-"+args(2)+".txt"
    new PrintWriter(filename) { write(res.trim); close }

  }

  def sort[A](coll: List[Iterable[A]])(implicit ordering: Ordering[A]) = coll.sorted

  def GenerateCandidates(Lk: List[scala.collection.SortedSet[String]]): List[scala.collection.SortedSet[String]]={
    def compare(A:scala.collection.SortedSet[String],B:scala.collection.SortedSet[String]):Boolean={
      if (A.dropRight(1).equals(B.dropRight(1)) && (A.last < B.last))
        true
      else
        false
    }
    def uni(A:scala.collection.SortedSet[String],B:scala.collection.SortedSet[String]):scala.collection.SortedSet[String] = { A++B}

    var Ck_1=List[scala.collection.SortedSet[String]]()//Seq(scala.collection.SortedSet("")).drop(1)
    for ((product , i) <- Lk.view.zipWithIndex)
      for (index <- Lk.drop(i+1)){
        if (compare(product,index)) {
          Ck_1= uni(product, index):: Ck_1
        }}
    for (c <- Ck_1)
      for (str <- c){
        val s = c - str
        if (! Lk.contains(s))
          Ck_1=Ck_1.filter(x=>x!=c)}
    Ck_1
  }


  def A_Priori(r:Iterator[Set[String]],threshold:Float)={
    val r1=r.toList
    var buf=List[scala.collection.SortedSet[String]]()
    var Lk=sort(r1.flatten.groupBy(identity).mapValues(_.size).filter{ case (key,value)=> value>= threshold}.keys
      .map(x=>scala.collection.SortedSet(x)).toList).map(x=>scala.collection.SortedSet[String]() ++ x.toSet)


    while (Lk.nonEmpty){
      buf = buf ::: Lk
      val Ck_1:List[scala.collection.SortedSet[String]] = GenerateCandidates(Lk)
      val count=r1.flatMap(x =>
      {
        var buf= List[scala.collection.SortedSet[String]]()
        for (candidate <- Ck_1)
          if (candidate.subsetOf(x)){
            buf = candidate :: buf}
        buf
      }).groupBy(identity).mapValues(_.size).filter(x=> x._2 >= threshold).keys.toList
      Lk=sort(count).map(x=>scala.collection.SortedSet[String]() ++ x.toSet)
      Lk
    }
    buf.toIterator}

}