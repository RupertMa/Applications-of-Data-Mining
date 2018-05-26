import java.io._
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.HashMap


object Community{
  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val conf=new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc=new SparkContext(conf)

    val input = sc.textFile(args(0)).filter(x=> ! x.contains("userID")).map(x=>{
      val four=x.split(",")
      (four(0).toLong,four(1).toLong)}).groupByKey().map(x=>(x._1,x._2.toArray)).persist()//.filter(x=>x._2.size>=7).map(x=>(x._1,x._2.toArray))
    val users=input.keys.collect().toSet   //action to input
    val data=input.collect().toMap  //action to input
    var pairs=List[(Long,Long)]()
    users.subsets(2).foreach(x=>pairs=(x.head,x.last)::pairs)
    val edges=sc.parallelize(pairs).flatMap{case(node1,node2)=>{
      var temp=List[(Long,Long)]()
      try{
        val products1=data(node1)
        val products2=data(node2)
        val condition=products1.intersect(products2)
        if (condition.length>=7) temp=(node1,node2)::(node2,node1)::temp
      }
      catch {case e:java.util.NoSuchElementException=>{}}
      temp
    }}


    val neighbors=edges.groupByKey().map(x=>(x._1,x._2.toArray)).persist()
    val neighbors_local=neighbors.collect().toMap   //action to neighbors
    val vertices=neighbors.keys.collect() //action to neighbors

    def bfs_paths(graph:Map[Long,Array[Long]],start:Long)={
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
      while (queue.nonEmpty){
        val currentNode=queue.last
        queue=queue.dropRight(1)
        for (next <- graph(currentNode)){ 
          if (level(next)==Int.MaxValue) {
            level+=(next->(level(currentNode)+1))
            queue=next::queue
          }
          if (level(next)==level(currentNode)+1) {
            num_ShortestPath+=(next->(num_ShortestPath(next)+num_ShortestPath(currentNode)))
            parents+=(next->(currentNode::parents(next)))
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
          val ups=parents(down._1) //parents of down
          val children=neighbors_local(down._1).diff(ups).filter(x=>level(x)>i) //children of down
          if (children.isEmpty){
            val temp=ups.map(up=>num_ShortestPath(up)).sum.toDouble
            btw=ups.map(up=>((up,down._1),1.0*num_ShortestPath(up)/temp)).toMap ++ btw
          }
          else{
            val credits=children.map(child=>btw(down._1,child)).sum
            val temp=ups.map(up=>num_ShortestPath(up)).sum.toDouble
            btw=ups.map(up=>((up,down._1),(1.0+credits)*num_ShortestPath(up)/temp)).toMap ++ btw
          }
        }
      }
      btw
    }



    def searchCommunity(graph:Map[Long,Array[Long]],start:Long,vertices:Array[Long])={
      var level=HashMap[VertexId,Int]()
      vertices.foreach(x=>{
        level+=(x->Int.MaxValue)
      })
      level+=(start->0)
      var queue=List(start)
      while (queue.nonEmpty){
        val currentNode=queue.last
        queue=queue.dropRight(1)
        for (next <- graph(currentNode)){
          if (level(next)==Int.MaxValue) {
            level+=(next->(level(currentNode)+1))
            queue=next::queue
          }
          if (level(next)==level(currentNode)+1) {
          }
        }
      }
      level.filter(_._2!=Int.MaxValue).keys
    }

    var weights=neighbors.keys.flatMap(x=>cal_btw(x)).map{case(key,value)=>{
      if (key._1>key._2) (key.swap,value)
      else (key,value)
    }}.groupByKey().map(x=>(x._1,x._2.sum/2.0)).collect()


    val btw_values=weights.map(_._2).sortWith(_>_).distinct //action to weights
    val m_2=weights.length * 2.0 
    var result=List[(Double,List[(Int,List[Long])])]()
    var degrees=neighbors_local.map(x=>(x._1,x._2.length.toDouble))
    var neighbors_copy=neighbors_local  //Aij is here


    for (value<-btw_values){
      var visited=List[Long]()
      var cc_2=List[(Int,List[Long])]()
      for ((user,index)<-neighbors_copy.keys.zipWithIndex){
        if (! visited.contains(user)) {
          val com=searchCommunity(neighbors_copy,user,vertices).toList
          cc_2=(index,com)::cc_2
          visited=com:::visited
        }}
      val communities=cc_2.flatMap { case (membership, members) => {
        var summation = List[Double]()
        val member_set = members.toSet
        for (pair <- member_set.subsets(2)) {
          if (neighbors_local(pair.head).contains(pair.last)) {
            summation = (1.0 - degrees(pair.head) * degrees(pair.last) / m_2) :: summation
          }
          else {
            summation = (0.0 - degrees(pair.head) * degrees(pair.last) / m_2) :: summation
          }
        }
        if (member_set.size == 1) summation = 0.0 :: summation
        summation
      }
      }.sum
      result=(communities/m_2,cc_2)::result
      val filtered=weights.filter(_._2==value).flatMap(x=>List(x._1))
      weights=weights.filter(_._2!=value)
      for ((userA,userB)<-filtered){
        neighbors_copy+=(userA->neighbors_copy(userA).filter(_!=userB))
        neighbors_copy+=(userB->neighbors_copy(userB).filter(_!=userA))
        degrees+=(userA->(degrees(userA)-1.0))
        degrees+=(userB->(degrees(userB)-1.0))
      }
    }


    val output=result.sortWith(_._1>_._1).head._2.map(x=>x._2.sorted).sortWith(_.head<_.head).map(x=>"["+x.mkString(",")+"]")

    val file = new File(args(1)+"Yibo_Ma_Community.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output.mkString("\n"))
    bw.close()
    val duration = (System.nanoTime - t0) / 1e9d
    println(s"Time: $duration sec")

  }
}
