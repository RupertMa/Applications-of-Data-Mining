import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.HashMap
import scala.math.Ordering.Implicits._


object JaccardLSH{
  def main(args:Array[String]): Unit ={
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(0)).filter(x=> ! x.contains("userID"))
    val data=input.map(x=>{
      val y=x.split(",")
      (y(0),y(1))
    }).groupByKey().persist()
    val products=data.flatMap(x=>x._2).distinct().collect()
    val num_bin=data.keys.collect().length
    val matrix_col=products.length
    val random=new scala.util.Random
    val vectors=data.map(x=> {
      val set=x._2.toSet
      var temp=List[Int]()
      for (i <- set ) {
        temp=products.indexOf(i)::temp 
      }
      var temp2=List[Int](x._1.toInt)
      for (a <- Range(0,54)) {  //number of permutations
        temp2=hash(x._1.toInt,random.nextInt(50),random.nextInt(50),2147483647,num_bin)::temp2
        }
      (temp2,temp)
    }).persist()

    val signatures=Array.fill[Int](55,matrix_col)(Int.MaxValue) //minhash signatures are produced
    vectors.collect().foreach(sth=>{
      for ((i,ind)<-sth._1.zipWithIndex)
        for (j<-sth._2){
          if (signatures(ind)(j) > i)
            signatures(ind)(j)=i
      }
    })



    def LSHashing(partition:Array[Array[Int]],r:Int)={
      var pairs=HashMap[Int,List[Int]]()
      val coefficient=(for (j<- 0 until r) yield random.nextInt(10)).toArray
      for (i <- 0 until matrix_col){
        val key=dot_product(getCol(i,partition),coefficient)
        if (! pairs.contains(key)) pairs= pairs + (key->List(i))
        else pairs= pairs + (key->(i::pairs(key)))
      }
      pairs
    }


    val r=5
    val b=11
    val sig=new Array[Array[Array[Int]]](b)
    for (i <- 0 until b)
      sig(i)=signatures.slice(i*r,(i+1)*r)

    val bands=sc.parallelize(sig).flatMap(x=>{
      val buckets=LSHashing(x,r)
      var buf=List[(Int,Int)]()
      for (bucket<-buckets.values)     
        for (i<-bucket.indices)
          for (j<-i+1 until bucket.length)
            buf = (bucket(i),bucket(j)) :: buf
      buf
    }).distinct()


    val product_index=vectors.values.collect()



    val result=bands.map(x=>{
      var count=0.0
      var count_x1=0.0
      var count_x2=0.0
      for (i<-product_index) {
        if (i.contains(x._1)) count_x1+=1
        if (i.contains(x._2)) count_x2+=1
        if (i.contains(x._1) && i.contains(x._2)) count += 1
      }
      ((products(x._1).toInt,products(x._2).toInt),count/(count_x1+count_x2-count))
    }).filter(x=>x._2>=0.5).map{ case (key,value)=>{
      if (key._1 < key._2) (key,value)
      else ((key._2,key._1),value)
    }}.collect()


    val str=result.sorted.map(x=>x._1._1+","+x._1._2+","+x._2).deep.mkString("\n")

    new PrintWriter(args(1)) { write(str); close }
    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")
  }

  def hash(x: Int, a: Int, b: Int, p: Int, m: Int): Int = ((a * x + b) % p) % m

  def getCol(n: Int, a: Array[Array[Int]]) = a.map{_(n)}

  def sort[A](coll: Array[(Int,Int)])(implicit ordering: Ordering[A]) = coll.sorted

  def dot_product(A1:Array[Int],A2:Array[Int])={
    var sum=0
    for (i <- A1.indices )
      sum+= A1(i)*A2(i)
    sum
  }

}
