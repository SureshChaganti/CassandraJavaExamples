package driverDemo;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import common.ConnectionUtil;
import common.Movies;

import java.io.IOException;

/**
 * Created by ryanknight on 7/12/16.
 */
public class SelectTestRunner {

    public void runSelectTest(String hostname) throws IOException {
        try (Cluster clusterConn = ConnectionUtil.connect(hostname)) {
            try (Session session = clusterConn.newSession()) {

                String query1 = "select * from udt.demo where solr_query='*:*'";
                runSelectTest(session, query1);
                System.out.println();
                System.out.println();

                String query2 = "select * from udt.demo where solr_query='{!tuple}name.firstname:Berenguer'";
                runSelectTest(session, query2);
                System.out.println();
                System.out.println();

                String query3 = "select * from udt.demo where solr_query='{!tuple}magic_numbers.field1:14'";
                runSelectTest(session, query3);
                System.out.println();
                System.out.println();

                String query4 = "select * from udt.demo where solr_query='{!tuple}friends.surname:Zasada'";
                runSelectTest(session, query4);
                System.out.println();
                System.out.println();

                String query5 = "select * from udt.demo where solr_query='({!tuple}friends.surname:Zasada AND {!tuple}friends.surname:Blasi)'";
                runSelectTest(session, query5);
                System.out.println();
                System.out.println();
            }
        }
    }

    private void runSelectTest(Session session, String query) {
        System.out.println("query = " + query);
        Statement statement = new SimpleStatement(query).setFetchSize(20);
        ResultSet resultSet = session.execute(statement);

        System.out.println("resultSet = " + resultSet + " size = " + resultSet.getAvailableWithoutFetching());
        resultSet.forEach(row -> {
            System.out.println("row = " + row);
        });
    }
}
