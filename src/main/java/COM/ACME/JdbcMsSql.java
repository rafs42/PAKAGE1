package COM.ACME;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;


public class JdbcMsSql {

    public static Connection connObj;
    public static String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=URBISBH;";

    public static void getDbConnection() {
        Connection conn = null;

        try {

//            String dbURL = "jdbc:sqlserver://localhost\\sqlexpress;integratedSecurity=true";
            String dbURL = JDBC_URL;
            String user = "URBISTAD";
            String pass = "urbistad";
            conn = DriverManager.getConnection(dbURL, user, pass);
            if (conn != null) {
                DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
                System.out.println("Driver name: " + dm.getDriverName());
                System.out.println("Driver version: " + dm.getDriverVersion());
                System.out.println("Product name: " + dm.getDatabaseProductName());
                System.out.println("Product version: " + dm.getDatabaseProductVersion());
            }
            String SQL = "SELECT ONOFF_IND, AS_OF_DATE FROM URBIS.DATES";
            Statement sta = conn.createStatement();
            ResultSet rs = sta.executeQuery(SQL);

            while (rs.next()) {
                System.out.println(rs.getString("ONOFF_IND") + " " + rs.getString("AS_OF_DATE"));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        getDbConnection();

    }
}
