package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Iterator;
import java.util.LinkedList;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import org.json.JSONObject;
import org.json.JSONArray;

public class FollowerServlet extends HttpServlet {
    //HBase attributes
    private static String zkAddr = "172.31.15.82";
    private static String tableName = "userlink";
    private static HTableInterface table;
    private static HConnection conn;
    private static byte[] bColFamily = Bytes.toBytes("follow");
    private final static Logger logger = Logger.getRootLogger();

    //MySQL attributes
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project34";
    private static final String URL = "jdbc:mysql://project3-4.cchoqihkcvdv.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;

    private static final String DB_USER = "wenhoulu";
    private static final String DB_PWD = "lwhlwh324324";
    private static Connection mySQLConn;

    public FollowerServlet() throws IOException, ClassNotFoundException, SQLException {
        //Initialize HBase
        logger.setLevel(Level.ERROR);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        conn = HConnectionManager.createConnection(conf);
        table = conn.getTable(Bytes.toBytes(tableName));
        
        //Initialize MySQL
        Class.forName(JDBC_DRIVER);
        mySQLConn = DriverManager.getConnection(URL, DB_USER, DB_PWD);

    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        try {
            String id = request.getParameter("id");
            JSONArray followers = new JSONArray();  //create a JSON array to store followers
            JSONObject result = new JSONObject();   //final result as a JSON object

            LinkedList followersList = new LinkedList();    //create a linked list to store follower's id
            
            //Execute HBase query to get all IDs of the followers
            Scan scan = new Scan();
            byte[] bCol1 = Bytes.toBytes("followee");
            byte[] bCol2 = Bytes.toBytes("followers");
            scan.addColumn(bColFamily, bCol1);
            scan.addColumn(bColFamily, bCol2);
            BinaryComparator comp = new BinaryComparator(Bytes.toBytes(id));
            Filter filter = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.EQUAL, comp);
            scan.setFilter(filter);
            scan.setBatch(10);
            ResultScanner rs = table.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                String oneFollower = Bytes.toString(r.getValue(bColFamily, bCol2));
                followersList.add(oneFollower);
            }
            rs.close();

            //Execute MySQL query to get corresponding name and profile url
            Statement stmt = mySQLConn.createStatement();
            Iterator it = followersList.iterator();
            String idList="";
            if (it.hasNext()) {
                idList = it.next().toString();
            }
            while (it.hasNext()) {
                idList += "' OR u_id='" + it.next().toString(); //concate query conditions
            }
            String sql = "SELECT name,url From userinfo WHERE u_id='" + idList + "' ORDER BY name,url";
            ResultSet rsSql = stmt.executeQuery(sql);
            while (rsSql.next()) {
                String name = rsSql.getString("name");
                String url = rsSql.getString("url");              
                JSONObject follower = new JSONObject();
                follower.put("name", name);
                follower.put("profile", url);
                followers.put(follower);
            }
            rsSql.close();
            
            result.put("followers", followers); //put into the final result
            PrintWriter writer = response.getWriter();
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
        } catch (SQLException ex) {

        }

    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }

}
