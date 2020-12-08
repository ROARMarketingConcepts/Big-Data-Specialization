[cloudera@quickstart ~]$ cd ExamplesOfAnalytics
[cloudera@quickstart ExamplesOfAnalytics]$ spark-shell --jars lib/gs-core-1.2.jar,lib/gs-ui-1.2.jar,lib/jcommon-1.0.16.jar,lib/jfreechart-1.0.13.jar,lib/breeze_2.10-0.9.jar,lib/breeze-viz_2.10-0.9.jar,lib/pherd-1.0.jar
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel).
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/zookeeper/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/parquet/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/jars/avro-tools-1.7.6-cdh5.4.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.0
      /_/

Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_67)
Type in expressions to have them evaluated.
Type :help for more information.
20/12/07 12:16:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
20/12/07 12:16:51 WARN SparkConf: 
SPARK_CLASSPATH was detected (set to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar').
This is deprecated in Spark 1.0+.

Please instead use:
 - ./spark-submit with --driver-class-path to augment the driver classpath
 - spark.executor.extraClassPath to augment the executor classpath
        
20/12/07 12:16:51 WARN SparkConf: Setting 'spark.executor.extraClassPath' to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar' as a work-around.
20/12/07 12:16:51 WARN SparkConf: Setting 'spark.driver.extraClassPath' to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar' as a work-around.
Spark context available as sc (master = local[*], app id = local-1607372223553).
20/12/07 12:17:22 WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
SQL context available as sqlContext.

scala> import org.apache.log4j.Logger; import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.Level

scala> Logger.getLogger("org").setLevel(Level.ERROR); Logger.getLogger("akka").setLevel(Level.ERROR)

scala> import org.apache.spark.graphx._; import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.rdd._

scala> import scala.io.Source
import scala.io.Source

scala> Source.fromFile("./EOADATA/metro.csv").getLines().take(5).foreach(println)
#metro_id,name,population
1,Tokyo,36923000
2,Seoul,25620000
3,Shanghai,24750000
4,Guangzhou,23900000

scala> Source.fromFile("./EOADATA/country.csv").getLines().take(5).foreach(println)
#country_id,name
1,Japan
2,South Korea
3,China
4,India

scala> Source.fromFile("./EOADATA/metro_country.csv").getLines().take(5).foreach(println)
#metro_id,country_id
1,1
2,2
3,3
4,3

scala> class PlaceNode(val name: String) extends Serializable
defined class PlaceNode

scala> case class Metro(override val name: String, population: Int) extends PlaceNode(name)
defined class Metro

scala> case class Country(override val name: String) extends PlaceNode(name)
defined class Country

scala> val metros: RDD[(VertexId, PlaceNode)] = sc.textFile("./EOADATA/metro.csv").
     | filter(! _.startsWith("#")). map {line =>
     | val row = line split ','
     | (0L + row(0).toInt, Metro(row(1), row(2).toInt)) }
metros: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, PlaceNode)] = MapPartitionsRDD[3] at map at <console>:40

scala> val countries: RDD[(VertexId, PlaceNode)] = sc.textFile("./EOADATA/country.csv").
     | filter(! _.startsWith("#")). map {line =>
     | val row = line split ','
     | (100L + row(0).toInt, Country(row(1))) }
countries: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, PlaceNode)] = MapPartitionsRDD[7] at map at <console>:40

scala> val mclinks: RDD[Edge[Int]] = sc.textFile("./EOADATA/metro_country.csv"). 
     | filter(! _.startsWith("#")). map {line =>
     | val row = line split ','
     | Edge(0L + row(0).toInt, 100L + row(1).toInt, 1) }
mclinks: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] = MapPartitionsRDD[11] at map at <console>:37

scala> val nodes = metros ++ countries
nodes: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, PlaceNode)] = UnionRDD[12] at $plus$plus at <console>:45

scala> val metrosGraph = Graph(nodes, mclinks)
metrosGraph: org.apache.spark.graphx.Graph[PlaceNode,Int] = org.apache.spark.graphx.impl.GraphImpl@d6f444b

scala> metrosGraph.vertices.take(5)
res4: Array[(org.apache.spark.graphx.VertexId, PlaceNode)] = Array((34,Metro(Hong Kong,7298600)), (52,Metro(Ankara,5150072)), (4,Metro(Guangzhou,23900000)), (16,Metro(Istanbul,14377018)), (28,Metro(Nagoya,9107000)))

scala> metrosGraph.edges.take(5)
res5: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1,101,1), Edge(2,102,1), Edge(3,103,1), Edge(4,103,1), Edge(5,104,1))

scala> metrosGraph.edges.filter(_.srcId == 1).map(_.dstId).collect()
res6: Array[org.apache.spark.graphx.VertexId] = Array(101)

scala> metrosGraph.edges.filter(_.dstId == 103).map(_.srcId).collect()
res7: Array[org.apache.spark.graphx.VertexId] = Array(3, 4, 7, 24, 34)

scala> metrosGraph.numEdges
res8: Long = 65

scala> metrosGraph.numVertices
res9: Long = 93

scala>  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = { if (a._2 > b._2) a else b
     | }
max: (a: (org.apache.spark.graphx.VertexId, Int), b: (org.apache.spark.graphx.VertexId, Int))(org.apache.spark.graphx.VertexId, Int)

scala>  def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = { if (a._2 <= b._2) a else b
     | }
min: (a: (org.apache.spark.graphx.VertexId, Int), b: (org.apache.spark.graphx.VertexId, Int))(org.apache.spark.graphx.VertexId, Int)

scala> metrosGraph.outDegrees.reduce(max)
res10: (org.apache.spark.graphx.VertexId, Int) = (5,1)

scala> metrosGraph.vertices.filter(_._1 == 5).collect()
res11: Array[(org.apache.spark.graphx.VertexId, PlaceNode)] = Array((5,Metro(Delhi,21753486)))

scala> metrosGraph.inDegrees.reduce(max)
res12: (org.apache.spark.graphx.VertexId, Int) = (108,14)

scala> metrosGraph.vertices.filter(_._1 == 108).collect()
res13: Array[(org.apache.spark.graphx.VertexId, PlaceNode)] = Array((108,Country(United States)))

scala> metrosGraph.outDegrees.filter(_._2 <= 1).count
res14: Long = 65                                                                

scala> metrosGraph.degrees.reduce(max)
res15: (org.apache.spark.graphx.VertexId, Int) = (108,14)                       

scala> metrosGraph.degrees.reduce(min)
res16: (org.apache.spark.graphx.VertexId, Int) = (34,1)

scala> metrosGraph.degrees.
     | 
     | filter { case (vid, count) => vid >= 100 }. 
     | 
     | map(t => (t._2,t._1)). 
     | 
     | groupByKey.map(t => (t._1,t._2.size)). 
     | 
     | sortBy(_._1).collect()
res19: Array[(Int, Int)] = Array((1,18), (2,4), (3,2), (5,2), (9,1), (14,1))

scala>  import breeze.linalg._; import breeze.plot._
import breeze.linalg._
import breeze.plot._

scala> def degreeHistogram(net: Graph[PlaceNode, Int]): Array[(Int, Int)] = net.degrees.
     | filter { case (vid, count) => vid >= 100 }. map(t => (t._2,t._1)).
     | groupByKey.map(t => (t._1,t._2.size)). sortBy(_._1).collect()
degreeHistogram: (net: org.apache.spark.graphx.Graph[PlaceNode,Int])Array[(Int, Int)]

scala> val nn = metrosGraph.vertices.filter{ case (vid, count) => vid >= 100 }.count()
nn: Long = 28

scala> val metroDegreeDistribution = degreeHistogram(metrosGraph).map({case(d,n) => (d,n.toDouble/nn)})
metroDegreeDistribution: Array[(Int, Double)] = Array((1,0.6428571428571429), (2,0.14285714285714285), (3,0.07142857142857142), (5,0.07142857142857142), (9,0.03571428571428571), (14,0.03571428571428571))

scala> val f = Figure()
f: breeze.plot.Figure = breeze.plot.Figure@725b219

scala> val p1 = f.subplot(2,1,0)
p1: breeze.plot.Plot = breeze.plot.Plot@44859b7a                                                                    ^

scala> val x = new DenseVector(metroDegreeDistribution map (_._1.toDouble)); val y = new DenseVector(metroDegreeDistribution map (_._2))
x: breeze.linalg.DenseVector[Double] = DenseVector(1.0, 2.0, 3.0, 5.0, 9.0, 14.0)
y: breeze.linalg.DenseVector[Double] = DenseVector(0.6428571428571429, 0.14285714285714285, 0.07142857142857142, 0.07142857142857142, 0.03571428571428571, 0.03571428571428571)

scala> p1.xlabel = "Degrees"
p1.xlabel: String = Degrees

scala> p1.ylabel = "Distribution"
p1.ylabel: String = Distribution

scala> p1 += plot(x, y)
res24: breeze.plot.Plot = breeze.plot.Plot@96b934a

scala> p1.title = "Degree distribution"
p1.title: String = Degree distribution

scala> val p2 = f.subplot(2,1,1)
p2: breeze.plot.Plot = breeze.plot.Plot@77859f95

scala> val metrosDegrees = metrosGraph.degrees.filter { case (vid, count) => vid >= 100 }.map(_._2).collect()
metrosDegrees: Array[Int] = Array(1, 9, 1, 14, 1, 1, 1, 5, 2, 1, 2, 1, 1, 1, 3, 1, 1, 1, 1, 2, 2, 3, 5, 1, 1, 1, 1, 1)

scala> p2.xlabel = "Degrees"
p2.xlabel: String = Degrees

scala> p2.ylabel = "Histogram of node degrees"
p2.ylabel: String = Histogram of node degrees

scala> p2 += hist(metrosDegrees, 20)
res25: breeze.plot.Plot = breeze.plot.Plot@77859f95

scala> 
