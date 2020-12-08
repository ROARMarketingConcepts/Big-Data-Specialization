[cloudera@quickstart ~]$ spark-shell
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
20/12/07 17:39:37 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
20/12/07 17:39:38 WARN SparkConf: 
SPARK_CLASSPATH was detected (set to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar').
This is deprecated in Spark 1.0+.

Please instead use:
 - ./spark-submit with --driver-class-path to augment the driver classpath
 - spark.executor.extraClassPath to augment the executor classpath
        
20/12/07 17:39:38 WARN SparkConf: Setting 'spark.executor.extraClassPath' to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar' as a work-around.
20/12/07 17:39:38 WARN SparkConf: Setting 'spark.driver.extraClassPath' to '/home/cloudera/Downloads/big-data-4/lib/spark-csv_2.10-1.5.0.jar:/home/cloudera/Downloads/big-data-4/lib/commons-csv-1.1.jar' as a work-around.
20/12/07 17:39:38 WARN Utils: Your hostname, quickstart.cloudera resolves to a loopback address: 127.0.0.1; using 10.0.2.15 instead (on interface eth0)
20/12/07 17:39:38 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Spark context available as sc (master = local[*], app id = local-1607391589961).
20/12/07 17:40:08 WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
SQL context available as sqlContext.

scala> import org.apache.log4j.Logger; import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.Level

scala>  import org.apache.spark.graphx._; import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.rdd._

scala> val airports: RDD[(VertexId, String)] = sc.parallelize( List((1L, "Los Angeles International Airport"),
     | (2L, "Narita International Airport"),
     | (3L, "Singapore Changi Airport"),
     | (4L, "Charles de Gaulle Airport"),
     | (5L, "Toronto Pearson International Airport")))
airports: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, String)] = ParallelCollectionRDD[0] at parallelize at <console>:35

scala> val flights: RDD[Edge[String]] = sc.parallelize( List(Edge(1L,4L,"AA1123"),
     | Edge(2L, 4L, "JL5427"), Edge(3L, 5L, "SQ9338"), Edge(1L, 5L, "AA6653"),
     | Edge(3L, 4L, "SQ4521")))
flights: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[String]] = ParallelCollectionRDD[1] at parallelize at <console>:35

scala> val flightGraph = Graph(airports, flights)
flightGraph: org.apache.spark.graphx.Graph[String,String] = org.apache.spark.graphx.impl.GraphImpl@7900ee0a

scala>  flightGraph.triplets.foreach(t => println("Departs from: " + t.srcAttr + " - Arrives at: " + t.dstAttr + " - Flight Number: " + t.attr))
Departs from: Los Angeles International Airport - Arrives at: Charles de Gaulle Airport - Flight Number: AA1123
Departs from: Los Angeles International Airport - Arrives at: Toronto Pearson International Airport - Flight Number: AA6653
Departs from: Narita International Airport - Arrives at: Charles de Gaulle Airport - Flight Number: JL5427
Departs from: Singapore Changi Airport - Arrives at: Charles de Gaulle Airport - Flight Number: SQ4521
Departs from: Singapore Changi Airport - Arrives at: Toronto Pearson International Airport - Flight Number: SQ9338

scala> case class AirportInformation(city: String, code: String)
defined class AirportInformation

scala> val airportInformation: RDD[(VertexId, AirportInformation)] = sc.parallelize( List((2L, AirportInformation("Tokyo", "NRT")),
     | (3L, AirportInformation("Singapore", "SIN")), (4L, AirportInformation("Paris", "CDG")),
     | (5L, AirportInformation("Toronto", "YYZ")), (6L, AirportInformation("London", "LHR")), (7L, AirportInformation("Hong Kong", "HKG"))))
airportInformation: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, AirportInformation)] = ParallelCollectionRDD[19] at parallelize at <console>:37

scala> def appendAirportInformation(id: VertexId, name: String, airportInformation: AirportInformation): String = name + ":"+ airportInformation.city
appendAirportInformation: (id: org.apache.spark.graphx.VertexId, name: String, airportInformation: AirportInformation)String

scala>  val flightJoinedGraph = flightGraph.joinVertices(airportInformation)(appendAirportInformation)
flightJoinedGraph: org.apache.spark.graphx.Graph[String,String] = org.apache.spark.graphx.impl.GraphImpl@2af4106f

scala> flightJoinedGraph.vertices.foreach(println)
(4,Charles de Gaulle Airport:Paris)
(1,Los Angeles International Airport)
(3,Singapore Changi Airport:Singapore)
(5,Toronto Pearson International Airport:Toronto)
(2,Narita International Airport:Tokyo)

scala> val flightOuterJoinedGraph = flightGraph.outerJoinVertices(airportInformation)((_,name, airportInformation) => (name, airportInformation))
flightOuterJoinedGraph: org.apache.spark.graphx.Graph[(String, Option[AirportInformation]),String] = org.apache.spark.graphx.impl.GraphImpl@72c4f1e9

scala> flightOuterJoinedGraph.vertices.foreach(println)
(4,(Charles de Gaulle Airport,Some(AirportInformation(Paris,CDG))))
(1,(Los Angeles International Airport,None))
(3,(Singapore Changi Airport,Some(AirportInformation(Singapore,SIN))))
(5,(Toronto Pearson International Airport,Some(AirportInformation(Toronto,YYZ))))
(2,(Narita International Airport,Some(AirportInformation(Tokyo,NRT))))

scala> val flightOuterJoinedGraphTwo = flightGraph.outerJoinVertices(airportInformation)((_, name, airportInformation) => (name, airportInformation.getOrElse(AirportInformation("NA","NA")))) flightOuterJoinedGraphTwo.vertices.foreach(println)
<console>:1: error: ';' expected but '.' found.
       val flightOuterJoinedGraphTwo = flightGraph.outerJoinVertices(airportInformation)((_, name, airportInformation) => (name, airportInformation.getOrElse(AirportInformation("NA","NA")))) flightOuterJoinedGraphTwo.vertices.foreach(println)
                                                                                                                                                                                                                        ^

scala> val flightOuterJoinedGraphTwo = flightGraph.outerJoinVertices(airportInformation)((_, name, airportInformation) => (name, airportInformation.getOrElse(AirportInformation("NA","NA")))); flightOuterJoinedGraphTwo.vertices.foreach(println)
(4,(Charles de Gaulle Airport,AirportInformation(Paris,CDG)))
(1,(Los Angeles International Airport,AirportInformation(NA,NA)))
(3,(Singapore Changi Airport,AirportInformation(Singapore,SIN)))
(5,(Toronto Pearson International Airport,AirportInformation(Toronto,YYZ)))
(2,(Narita International Airport,AirportInformation(Tokyo,NRT)))
flightOuterJoinedGraphTwo: org.apache.spark.graphx.Graph[(String, AirportInformation),String] = org.apache.spark.graphx.impl.GraphImpl@3b45c1bb

scala> case class Airport(name: String, city: String, code: String)
defined class Airport

scala> val flightOuterJoinedGraphThree = flightGraph.outerJoinVertices(airportInformation)((_, name, b) => b match {
     | case Some(airportInformation) => Airport(name, airportInformation.city, airportInformation.code)
     | case None => Airport(name, "", "") })
flightOuterJoinedGraphThree: org.apache.spark.graphx.Graph[Airport,String] = org.apache.spark.graphx.impl.GraphImpl@3f5eeb0a

scala> flightOuterJoinedGraphThree.vertices.foreach(println)
(4,Airport(Charles de Gaulle Airport,Paris,CDG))
(1,Airport(Los Angeles International Airport,,))
(3,Airport(Singapore Changi Airport,Singapore,SIN))
(5,Airport(Toronto Pearson International Airport,Toronto,YYZ))
(2,Airport(Narita International Airport,Tokyo,NRT))

scala> 
