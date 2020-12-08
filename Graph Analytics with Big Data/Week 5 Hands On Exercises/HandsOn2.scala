scala> Source.fromFile("./EOADATA/continent.csv").getLines().take(5).foreach(println)
#continent_id,name
1,Asia
2,Africa
3,North America
4,South America

scala>  Source.fromFile("./EOADATA/country_continent.csv").getLines().take(5).foreach(println)
#country_id,continent_id
1,1
2,1
3,1
4,1

scala> case class Continent(override val name: String) extends PlaceNode(name)
defined class Continent

scala> val continents: RDD[(VertexId, PlaceNode)] = sc.textFile("./EOADATA/continent.csv").
     | 
     | filter(! _.startsWith("#")).
     | 
     | map {line =>
     | 
     | val row = line split ','
     | 
     | (200L + row(0).toInt, Continent(row(1)))
     | 
     | }
continents: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, PlaceNode)] = MapPartitionsRDD[88] at map at <console>:49

scala> 

scala> 

scala> val cclinks: RDD[Edge[Int]] = sc.textFile("./EOADATA/country_continent.csv").
     | filter(! _.startsWith("#")). map {line =>
     | val row = line split ','
     | Edge(100L + row(0).toInt, 200L + row(1).toInt, 1) }
cclinks: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] = MapPartitionsRDD[92] at map at <console>:43

scala> val cnodes = metros ++ countries ++ continents
cnodes: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, PlaceNode)] = UnionRDD[94] at $plus$plus at <console>:55

scala> val clinks = mclinks ++ cclinks
clinks: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] = UnionRDD[95] at $plus$plus at <console>:46

scala> val countriesGraph = Graph(cnodes, clinks)
countriesGraph: org.apache.spark.graphx.Graph[PlaceNode,Int] = org.apache.spark.graphx.impl.GraphImpl@698adc93

scala> import org.graphstream.graph.implementations._
import org.graphstream.graph.implementations._

scala> val graph: SingleGraph = new SingleGraph("countriesGraph")
graph: org.graphstream.graph.implementations.SingleGraph = countriesGraph

scala> graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)") graph.addAttribute("ui.quality")
<console>:1: error: ';' expected but '.' found.
       graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)") graph.addAttribute("ui.quality")
                                                                                ^

scala> graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)"); graph.addAttribute("ui.quality");

scala> graph.addAttribute("ui.antialias")


scala> for ((id:VertexId, place:PlaceNode) <- countriesGraph.vertices.collect()) 
     | {
     | val node = graph.addNode(id.toString).asInstanceOf[SingleNode]; node.addAttribute("name", place.name); node.addAttribute("ui.label", place.name)
     | if (place.isInstanceOf[Metro])
     | node.addAttribute("ui.class", "metro")
     | else if(place.isInstanceOf[Country])
     | node.addAttribute("ui.class", "country")
     | else if(place.isInstanceOf[Continent])
     | node.addAttribute("ui.class", "continent")
     | }

scala> for (Edge(x,y,_) <- countriesGraph.edges.collect()) { graph.addEdge(x.toString ++ y.toString, x.toString, y.toString,
     | true).asInstanceOf[AbstractEdge] }

scala> graph.display()
res34: org.graphstream.ui.swingViewer.Viewer = org.graphstream.ui.swingViewer.Viewer@414f0cf9

scala> 
