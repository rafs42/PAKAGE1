package COM.ACME;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.ResultSet;
        import java.sql.SQLException;
        import java.sql.Statement;
        import java.sql.*;
        import java.util.Arrays;


public class BulkRowInsertJDBC {

    public static Connection connObj;
    //public static String JDBC_URL = "jdbc:sqlserver://10.23.1.44:64451;databaseName=URBILATD1;";
    static String sql = "INSERT INTO [URBIS].[ABC_INCENTIVE_HDR]   ([FILE_SEQNO],[FILE_NAME] ,[FILE_TYPE] ,[STATUS1] ,[LOAD_DATE]) VALUES  (";
    static String sql_insert = "INSERT INTO [URBIS].[ABC_INCENTIVE_HDR]   ([FILE_SEQNO],[FILE_NAME] ,[STATUS1]) VALUES  (?,?,?)";
    static PreparedStatement pstmt = null;

    public static String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=URBISBH;";


    public static void getDbConnection() {
        Connection conn = null;
        try {
//          String dbURL = "jdbc:sqlserver://localhost\\sqlexpress;integratedSecurity=true";
            /*String dbURL = JDBC_URL;
            String user = "INTF_APPS1";
            String pass = "INTF_APPS1";*/

            String dbURL = JDBC_URL;
            String user = "urbistad";
            String pass = "manager";
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

            pstmt = conn.prepareStatement(sql_insert) ;
            pstmt.setString(2, "mkyong");
            pstmt.setInt(1, 4);
            pstmt.setString(3, "L");
            pstmt.addBatch();

            int row = pstmt.executeUpdate();
            System.out.println(row);

            pstmt = conn.prepareStatement(sql_insert) ;
            pstmt.setString(2, "mkyong");
            pstmt.setInt(1, 14);
            pstmt.setString(3, "L");
            pstmt.addBatch();

            pstmt.setString(2, "mkyong");
            pstmt.setInt(1, 15);
            pstmt.setString(3, "L");
            pstmt.addBatch();

            int[] rows2 = pstmt.executeBatch();

            System.out.println(Arrays.toString(rows2));

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
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

