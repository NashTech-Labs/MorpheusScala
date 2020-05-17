# MorpheusScala

This project demonstrates the steps to bring Property Graphs and its querying to spark platform. It creates two nodes - actors and movies and edges between them based on the relationship ACTED_IN. Every actor node will have an edge to the movie node he worked in. The data sets for the nodes and edge is given in:
1. persons.csv
2. movies.csv
3. acted_in.csv

The project contains a main object MorpheusScala and a helper object MovieActorDataFrames.

1. MovieActorDataFrames contains a method to create data frames from csv files. 

2. MorpheusScala contains the code to create Morpheus Session. Using the Morpheus sessionl, you can create nodes, edges and the resulting graph. Once the graph is created, different Cypher queries are written in the code to process the graph.
