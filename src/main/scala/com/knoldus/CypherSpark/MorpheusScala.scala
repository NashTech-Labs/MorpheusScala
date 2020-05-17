package com.knoldus.CypherSpark

import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.value.CypherValue


object MorpheusScala extends App {

    //Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master","local[*]")
      .getOrCreate()

    //Create a Morpheus session
    implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)

    //Read csv files into data frame
    val moviesDF = MovieActorDataFrames.createNode(spark, "movies.csv")
    val personsDF = MovieActorDataFrames.createNode(spark, "persons.csv")
    val actedInDF = MovieActorDataFrames.createNode(spark, "acted_in.csv")

    //Create element mapping for movies node
    val movieNodeMapping = NodeMappingBuilder
      .withSourceIdKey("id:Int")
      .withImpliedLabel("Movies")
      .withPropertyKey(propertyKey = "title", sourcePropertyKey = "title")
      .withPropertyKey(propertyKey = "tagline", sourcePropertyKey = "tagline")
      .withPropertyKey(propertyKey = "summary", sourcePropertyKey = "summary")
      .withPropertyKey(propertyKey = "poster_image", sourcePropertyKey = "poster_image")
      .withPropertyKey(propertyKey = "duration", sourcePropertyKey = "duration")
      .withPropertyKey(propertyKey = "rated", sourcePropertyKey = "rated")
      .build

    //Create element mapping for person node
    val personNodeMapping = NodeMappingBuilder
      .withSourceIdKey("id:Int")
      .withImpliedLabel("Person")
      .withPropertyKey("name", "name")
      .withPropertyKey("born", "born")
      .withPropertyKey("poster_image", "poster_image")
      .build

    //Create element mapping for the edge between two nodes
    val actedInRelationMapping = RelationshipMappingBuilder
        .withSourceIdKey("rel_id:Int")
        .withSourceStartNodeKey("START_ID")
        .withSourceEndNodeKey("END_ID")
        .withRelType("ACTED_IN")
        .withPropertyKey("role", "role")
        .build

    //Create Nodes and Relationships
    val moviesNode = MorpheusElementTable.create(movieNodeMapping, moviesDF)
    val personsNode = MorpheusElementTable.create(personNodeMapping, personsDF)
    val actedInRelation = MorpheusElementTable.create(actedInRelationMapping, actedInDF)

    //Create Property Graph
    val actorMovieGraph = morpheus.readFrom(personsNode,moviesNode,actedInRelation)


    //Query to get all Person nodes
    val actors = actorMovieGraph.cypher(
        "MATCH (p:Person) return p.name AS Actor_Name"
    )
    actors.records.show

    //Query to get all Movie nodes
    val movies = actorMovieGraph.cypher(
        "MATCH (m:Movies) return m.title AS Movie_Titles"
    )
    movies.records.show

    //Query to read all actors and their respective movie
    val actor_movies = actorMovieGraph.cypher(
        "MATCH (p:Person)-[a:ACTED_IN]->(m:Movies) RETURN p.name AS ACTOR_NAME, m.title AS MOVIE_TITLE"
    )
    actor_movies.records.show

    //Query to read movie belonging to a particular actor
    val movie = actorMovieGraph.cypher(
        "MATCH (p:Person{name:'Gloria Foster'}) -[a:ACTED_IN]->(m:Movies) RETURN m.title AS MOVIE_TITLE"
    )
    movie.records.show

    //Query with Parameter Substitution
    val param =  CypherValue.CypherMap(("movie_name", "The Matrix Revolutions"))
    val actorName = actorMovieGraph.cypher(
        s"MATCH (m:Movies{title:{movie_name}})<-[a:ACTED_IN]-(p:Person) RETURN p.name AS ACTOR_NAME",
        param)
    actorName.records.show

}
