package at.htl.Lengauer_JDBC;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class Lengauer_JDBCTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJDBC(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n"+e.getMessage() +"\n");
            System.exit(1);
        }

        try {
            Statement statement = conn.createStatement();
            statement.execute("CREATE TABLE CINEMA (" +
                    "ID      INTEGER CONSTRAINT  cinema_pk primary key GENERATED ALWAYS AS IDENTITY," +
                    "NAME    VARCHAR(255)," +
                    "ADDRESS VARCHAR(255)" +
                    ")");
            statement.execute("CREATE TABLE HALL (" +
                    "ID INTEGER CONSTRAINT hall_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                    "DESCRIPTION VARCHAR(255)," +
                    "SEATING     INTEGER default 0 not null," +
                    "CINEMA_ID   INTEGER" +
                    "   CONSTRAINT hall_cinema_fk" +
                    "   REFERENCES CINEMA(id)" +
                    ")"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @AfterClass
    public static void teardownJdbc(){
        try{
            if(conn != null && !conn.isClosed()){
                Statement statement = conn.createStatement();
                statement.execute("DROP TABLE HALL");
                statement.execute("DROP TABLE CINEMA");
                conn.close();
                System.out.println("Good Bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t01_dml(){
        int count = 0;
        try {
            PreparedStatement cinemaInsert = conn.prepareStatement("INSERT INTO CINEMA(NAME, ADDRESS) VALUES (?,?)");
            cinemaInsert.setString(1, "Cineplexx World Linz");
            cinemaInsert.setString(2, "Prinz-Eugen-Straße 22, 4020 Linz");
            count += cinemaInsert.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(count, is(1));

        count = 0;
        try {
            PreparedStatement cinemaInsert = conn.prepareStatement(
                    "INSERT INTO HALL(DESCRIPTION, SEATING, CINEMA_ID) " +
                    "VALUES (?,?,(SELECT id FROM CINEMA WHERE name = ?))");
            cinemaInsert.setString(1, "DOLBY CINEMA");
            cinemaInsert.setInt(2, 394);
            cinemaInsert.setString(3, "Cineplexx World Linz");
            count += cinemaInsert.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(count, is(1));
    }
    @Test
    public void t02_MetaData(){

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet res = metaData.getColumns(null, null, "CINEMA", null);

            res.next();
            assertThat(res.getString(4), is("ID"));
            assertThat(res.getInt(5), is(Types.INTEGER));
            res.next();
            assertThat(res.getString(4), is("NAME"));
            assertThat(res.getInt(5), is(Types.VARCHAR));
            res.next();
            assertThat(res.getString(4), is("ADDRESS"));
            assertThat(res.getInt(5), is(Types.VARCHAR));

            res = metaData.getPrimaryKeys(null, null, "CINEMA");
            res.next();
            assertThat(res.getString(4), is("ID"));

            res = metaData.getColumns(null, null, "HALL", null);
            res.next();
            assertThat(res.getString(4), is("ID"));
            assertThat(res.getInt(5), is(Types.INTEGER));
            res.next();
            assertThat(res.getString(4), is("DESCRIPTION"));
            assertThat(res.getInt(5), is(Types.VARCHAR));
            res.next();
            assertThat(res.getString(4), is("SEATING"));
            assertThat(res.getInt(5), is(Types.INTEGER));

            res = metaData.getPrimaryKeys(null, null, "HALL");
            res.next();
            assertThat(res.getString(4), is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

}
