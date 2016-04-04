#Load the Movie Lens Data into Cassandra

* Create the schema for the movie data using the cql in conf/movie_db.cql
`cqlsh node0 -f movie_db.cql`

* Build the jar file
`mvn package`

* Run the java program to import the data
`java -jar CassandraJavaExamples-0.1-jar-with-dependencies.jar load ml-10M100K node0`

* Use cqlsh to verify the data was loaded

* Run the java program to page through the data
`java -jar CassandraJavaExamples-0.1-jar-with-dependencies.jar view node0`

* Run the java mapping example to view information about a movie
`java -jar CassandraJavaExamples-0.1-jar-with-dependencies.jar map 1193 node0`

* Connect DevCenter to cluster

#Making the Movie Lens data searchable with Solr

* Use the dsetool to create a solr core of the movies:

`dsetool create_core movie_db.movies reindex=true generateResources=true`

* Verify the Solr core was created by browsing to the [Solr Management Interface](http://localhost:8983/solr) and trying some basic queries.

* Experiment with different Solr Queries in the management interface.

* In cqlsh you can also run some queries, i.e.:

`select * from movies where solr_query = 'categories:Drama';`

`select * from movies where solr_query = '{"q":"categories:Drama","facet":{"field":"title"}}';`

`select * from movies where solr_query = '{"q":"categories:*","facet":{"field":"categories"}}';`

For more example queries see the [DSE Search Tutorial](http://docs.datastax.com/en/datastax_enterprise/4.7/datastax_enterprise/srch/srchTutCQL.html)
