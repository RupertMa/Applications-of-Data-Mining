import java.io._
import scala.collection.mutable.Queue
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.HashMap

object Betweenness{
  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)
    val input = sc.textFile(args(0)).filter(x=> ! x.contains("userID")).map(x=>{
      val four=x.split(",")
      (four(0).toLong,four(1).toLong)}).groupByKey().filter(x=>x._2.size>=7).persist()

    val users=input.keys.collect().toSet   //action to input
    val data=input.collectAsMap() //action to input
    val num_users=users.size
    var pairs=List[(Long,Long)]()
    users.subsets(2).foreach(x=>pairs=(x.head,x.last)::pairs)
    val edges=sc.parallelize(pairs).flatMap{case(node1,node2)=>{
      var temp=List[(Long,Long)]()
      try{
        val products1=data(node1).toArray
        val products2=data(node2).toArray
        val condition=products1.intersect(products2)
        if (condition.length>=7) temp=(node1,node2)::(node2,node1)::temp
      }
      catch {case e:java.util.NoSuchElementException=>{}}
      temp
    }}


    val neighbors=edges.groupByKey().map(x=>(x._1,x._2.toArray)).persist()
    val neighbors_local=neighbors.collect().toMap   //action to neighbors
    val vertices=neighbors.keys.collect() //action to neighbors

    def bfs_paths(graph: scala.collection.Map[Long,Array[Long]],start:Long)={
      var level=HashMap[Long,Int]()
      var num_ShortestPath=HashMap[Long,Int]()
      var parents=HashMap[Long,List[Long]]()
      vertices.foreach(x=>{
        level+=(x->Int.MaxValue)
        num_ShortestPath+=(x->0)
        parents+=(x->List())
      })
      level+=(start->0)
      num_ShortestPath+=(start->1)
      var queue=List(start)
      //val queue=Queue(start)
      while (queue.nonEmpty){
        val currentNode=queue.last
        queue=queue.dropRight(1)
        //val currentNode=queue.dequeue()
        for (next <- graph(currentNode)){ 
          if (level(next)==Int.MaxValue) {
            level+=(next->(level(currentNode)+1))
            //level(next)=level(currentNode)+1
            queue=next::queue
            //queue+=next
          }
          if (level(next)==level(currentNode)+1) {
            //if (currentNode==start) num_ShortestPath+=(next->(num_ShortestPath(next)+1))
            //else num_ShortestPath+=(next->(num_ShortestPath(next)+num_ShortestPath(currentNode)))
            num_ShortestPath+=(next->(num_ShortestPath(next)+num_ShortestPath(currentNode)))
            parents+=(next->(currentNode::parents(next)))
            //queue=(next,next::path)::queue
          }
        }
      }
      (level.filter(x=>x._2!=Int.MaxValue) ,num_ShortestPath,parents)
    }

    def cal_btw(root:Long)={
      val (level,num_ShortestPath,parents)=bfs_paths(neighbors_local,root)
      val depth=level.values.max
      var btw=Map[(Long,Long),Double]() 
      for (i<- depth to 1 by -1){
        val downs=level.filter(_._2==i) 
        for (down<-downs){
          val ups=parents(down._1) //parents of down  //must have parents
          val children=neighbors_local(down._1).diff(ups).filter(x=>level(x)>i) //children of down //tested np
          if (children.isEmpty){
            val temp=ups.map(up=>num_ShortestPath(up)).sum.toDouble
            btw=ups.map(up=>{
              ((up,down._1),1.0*num_ShortestPath(up)/temp)
            }).toMap ++ btw
          }
          else{
            val credits=children.map(child=>btw(down._1,child)).sum 
            val temp=ups.map(up=>num_ShortestPath(up)).sum.toDouble
            btw=ups.map(up=>{
              ((up,down._1),(1.0+credits)*num_ShortestPath(up)/temp)
            }).toMap ++ btw
          }
        }
      }
      btw
    }

    val weights=neighbors.keys.flatMap(x=>cal_btw(x)).map{case(key,value)=>{
      if (key._1>key._2) (key.swap,value)
      else (key,value)
    }}.reduceByKey((x,y)=>x+y).map(x=>(x._1._1,x._1._2,x._2/2.0)).collect()
    
    val x=weights.sorted.deep.mkString("\n")
    val file = new File(args(1)+"Yibo_Ma_Betweenness.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(x)
    bw.close()
    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")
  }
}
