package driverDemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import common.ConnectionUtil;
import common.Movies;

import java.io.IOException;

public class MappingDemo {

    public void viewMovieData(int movie_id, String hostname) throws IOException {
        try (Cluster clusterConn = ConnectionUtil.connect(hostname)) {
            try (Session session = clusterConn.newSession()) {
                MappingManager manager = new MappingManager(session);
                Mapper<Movies> moviesMapper = manager.mapper(Movies.class);
                Movies movieData = moviesMapper.get(movie_id);
                System.out.println("movie data = " + movieData);

            }
        }
    }
}
