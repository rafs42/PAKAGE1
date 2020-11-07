package COM.ACME;

import org.apache.commons.lang.ObjectUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.*;
import java.text.DateFormat;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;



public class LoadIncentiveFile {
    static String myVersion = "SwiftUnixNormaliserV3  version :  15AUG2018";
    static String SOURCEPATH ="E:\\Temp\\ILA_Services\\INCENTIVE\\UNPROCESSED\\";
    static String DESTPATH = "E:\\Temp\\ILA_Services\\INCENTIVE\\PROCESSED\\";
    static String LOGFILELOCATION = "E:\\Temp\\ILA_Services\\INCENTIVE\\LOG\\";
    static String BACKUPPATH = "E:\\Temp\\ILA_Services\\INCENTIVE\\BACKUPPATH\\";
    static File    directory;
    static String[] fileNames;
    static String UNIXTODOS = "N";
    static String DIRCHAR = "\\";
    static String DESTPREFIX = "DONE_";
    static String lineseperator;
    static char CR  = (char) 0x0D;
    static char LF  = (char) 0x0A;
    static String CRLF  = "" + CR + LF;

    static byte[] buffer = new byte[1024];
    static BufferedInputStream bis;
    static File newFile;
    static OutputStream os;
    static BufferedOutputStream bos;
    static int readCount;
    static int		vFILE_SEQNO;
    static String	vFILE_NAME;
    static String	vSTATUS1;
    static int		vLOAD_DATE;

    static Properties prop = new Properties();
    static InputStream input = null;
    static int vstep = 0;
    static String sql1 = "INSERT INTO [REPORT].[ABC_INCENTIVE_HDR]   ([FILE_SEQNO],[FILE_NAME] ,[FILE_TYPE] ,[STATUS1] ,[LOAD_DATE]) VALUES  (";
    static PreparedStatement pstmt = null;

    public static Connection connObj;
    public static Connection conn = null;
    public static String JDBC_URL = "jdbc:sqlserver://10.23.1.44:64451;databaseName=URBILATD1;";
    static String sql_hdr_insert = "INSERT INTO [URBIS].[ABC_INCENTIVE_HDR]   ([FILE_SEQNO],[FILE_NAME] ,[FILE_TYPE] , [STATUS1]) VALUES  (?,?,'INCENTIVE', 'L')";
    static String sql_get_FILE_SEQNO= "SELECT MAX(FILE_SEQNO)+1 AS SEQNO FROM [URBIS].[ABC_INCENTIVE_HDR]";
    static String sql_get_hdr_row_count= "SELECT COUNT(*) AS ROWCOUNT FROM [URBIS].[ABC_INCENTIVE_HDR]";
    static String sql_dtl_insert = "INSERT INTO [URBIS].[ABC_INCENTIVE_DTL] ([FILE_SEQNO] ,[FILE_NAME] ,[LINE_NO] ,[LINE_TXT] ,[STATUS1]) VALUES (?, ?, ?, ?, 'L')";

    public static boolean getDbConnection() {

        try {

/*          String dbURL = "jdbc:sqlserver://localhost\\sqlexpress;integratedSecurity=true";
            String dbURL = JDBC_URL;
            String user = "INTF_APPS1";
            String pass = "INTF_APPS1";*/

            String JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=URBISBH;";
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

                String SQL = "SELECT ONOFF_IND, AS_OF_DATE FROM URBIS.DATES";
                Statement sta = conn.createStatement();
                ResultSet rs = sta.executeQuery(SQL);

                while (rs.next()) {
                    System.out.println(rs.getString("ONOFF_IND") + " " + rs.getString("AS_OF_DATE"));
                }

                return(true);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

        return(true);
    }

    private static String CDateTime(String strFormat) {

        if (strFormat.length() < 1) {
            strFormat = "yyyy/MM/dd HH:mm:ss";
        }

        DateFormat dateFormatter = new SimpleDateFormat(strFormat);
        java.util.Date dtCurrDate = new java.util.Date();
        String strDateTime = dateFormatter.format(dtCurrDate);

        return (strDateTime);
    }

    private static void Update_Log(String Log_Dir, String Msg) {

        PrintWriter log = null;
        String Spaces = "          ";

        try {
            log = new PrintWriter(new FileWriter(Log_Dir +  "SwiftUnixNormaliserV2"	+ CDateTime("yyyyMMdd") + ".log", true));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        if (Msg.length() < 1) {
            log.println("------------------------------------------------------------------");
            log.println("\n");
        } else {
            log.println(CDateTime("yyyy/MM/dd hh:mm:ss") + Spaces + Msg);
        }
        log.close();

    }

    private static void Process(String fName, String nName, String rName) {
        // read file content
        String sCurrentLine;
        String completeMessageString = "";
        FileReader FR;
        BufferedReader br;
        int nFileSeqno=0;
        int vRowCount=0;
        int row;
        int vlineno=1;
        ResultSet rs;
        Statement sta;

        Update_Log(LOGFILELOCATION, "In Process   fName=" +  fName + "     nName=" + nName + "    rName=" + rName);
        System.out.println("In Process   fName=" +  fName + "     nName=" + nName + "    rName=" + rName);

        try {
            //try reading file of requests into an arraylist
            List<String> AllLines = null;
            try {
                AllLines = Files.readAllLines(new File(fName.trim()).toPath(), Charset.forName("UTF-8"));
            } catch (IOException e) {
                System.out.println("fail reading the  file." + fName.trim());
                return;
            }

            //GET NEXT FILE_SEQNO
           sta = conn.createStatement();
           rs = sta.executeQuery(sql_get_hdr_row_count);

            while (rs.next()) {
                nFileSeqno=rs.getInt("ROWCOUNT");
                System.out.println("ROWCOUNT " + vRowCount );
            }

            if (vRowCount=0) {
                nFileSeqno = 0;
            } else {
                rs = sta.executeQuery(sql_get_FILE_SEQNO);
                while (rs.next()) {
                    nFileSeqno = rs.getInt("SEQNO");
                    System.out.println("SEQNO " + nFileSeqno);
                }
            }

            //Insert into header
            pstmt = conn.prepareStatement(sql_hdr_insert) ;
            pstmt.setString(2, fName);
            pstmt.setInt(1, nFileSeqno);
            pstmt.addBatch();
            row = pstmt.executeUpdate();
            System.out.println("Header Rows Inserted=" + row);

            //insert into details
            pstmt = conn.prepareStatement(sql_dtl_insert) ;
            for (String aReq : AllLines) {
                System.out.println(aReq);
                pstmt.setInt(1, nFileSeqno);
                pstmt.setString(2, fName);
                pstmt.setInt(3, vlineno);
                pstmt.setString(4, aReq);
                pstmt.addBatch();
                vlineno++;
            }
            int[] rows2 = pstmt.executeBatch();
            System.out.println("Detail Rows Inserted=" + Arrays.toString(rows2));


            //rename file
            File ofile = new File(fName.trim());
            File rfile = new File(rName.trim());
            /*boolean chkrename = ofile.renameTo(rfile);

            if (!chkrename) {
                Update_Log(LOGFILELOCATION, " Error Renaming :" + fName + " to "  + rName);
                //System.out.println(" Renaming error see log");
                return;
            }*/


        } catch (Exception e) {
            e.printStackTrace();
            Update_Log(LOGFILELOCATION, " Error " + e.getMessage());
            Update_Log(LOGFILELOCATION, " Error " + e.toString());
            System.out.flush();
            System.exit(0);
        }

    }

    private static void Processdir(String aDir, String dDir) {
        File dir = new File(aDir);
        if (dir.isDirectory()) {

            String[] dirlist = dir.list();

            Update_Log(LOGFILELOCATION, "In Processdir SOURCEPATH=  " + SOURCEPATH  + " list count=" + dirlist.length);
            Update_Log(LOGFILELOCATION, "In Processdir aDir=" + aDir  + "   dDir=" + dDir);
            Update_Log(LOGFILELOCATION, " ");
            System.out.println("In Processdir aDir=" + aDir  + "   dDir=" + dDir);
            System.out.println("In Processdir SOURCEPATH=  " + SOURCEPATH  + " list count=" + dirlist.length);

            for (int i = 0; i < dirlist.length; i++) {
                System.out.println("In Processdir proceessing:" + dirlist[i]);
                Process(aDir +  dirlist[i], "", DESTPATH   + "BAK_" + dirlist[i]);
            }
        }
        else {
            return;
        }
    }

    public static void main(String[] args) {

        //lets see how many files we have to send
        directory = new File(SOURCEPATH);
        fileNames = directory.list();

        if (fileNames.length < 1)
        {
            System.out.println("No files available in source directory to Upload");
            Update_Log(LOGFILELOCATION, "No files available in source directory to Upload" );
            Update_Log(LOGFILELOCATION, "" );
            System.exit(0);
        }

        //check db is available
        boolean ChkDBConnection = getDbConnection();

        if (!ChkDBConnection) {
            Update_Log(LOGFILELOCATION, "DB Connection not available" );
            Update_Log(LOGFILELOCATION, "" );
            System.exit(0);
        }

        // read  and process all files in source directory
        Processdir(SOURCEPATH, DESTPATH);

        //close any db connections
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
