
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Example {

  public static void main(String... args) throws Exception {
    //Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://localhost?user=Administrator&password=password");


    Connection connection = DriverManager.getConnection("jdbc:couchbase:analytics://10.145.213.101", "Administrator", "password");

    PreparedStatement ps = connection.prepareStatement("select * from airline where country = ? limit 10");


   /* Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select \"foo\"");
    System.err.println(resultSet);

    resultSet.next();
    System.err.println(resultSet.getString(0));

    statement.close();*/
    connection.close();

  }
}
