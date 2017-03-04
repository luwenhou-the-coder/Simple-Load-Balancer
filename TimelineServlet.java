package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

import org.json.JSONArray;
import org.json.JSONObject;

public class TimelineServlet extends HttpServlet {

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
    //MongoDB attributes
    private static MongoClient mongoClient;
    private static MongoDatabase db;

    public TimelineServlet() throws Exception {
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
        //Initialize HBase
        Class.forName(JDBC_DRIVER);
        mySQLConn = DriverManager.getConnection(URL, DB_USER, DB_PWD);

        //Initialize MongoDB
        mongoClient = new MongoClient("172.31.10.25", 27017);
        db = mongoClient.getDatabase("project34");
    }

    public static JSONObject getProfile(String id, JSONObject result) throws SQLException {
        //Get user's name and profile url
        Statement stmt = mySQLConn.createStatement();
        String sql = "SELECT name,url From userinfo WHERE u_id='" + id + "'";
        ResultSet rsSql = stmt.executeQuery(sql);
        if (rsSql.next()) {
            String name = rsSql.getString("name");
            String url = rsSql.getString("url");
            //If YES, send back the user's Name and Profile Image URL.
            result.put("name", name);
            result.put("profile", url);
        }
        rsSql.close();
        return result;
    }

    public static JSONObject getFollowers(String id, JSONObject result) throws SQLException, IOException {
        //Get follower's name and profile url
        JSONArray followers = new JSONArray();
        LinkedList followersList = new LinkedList();
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

        Statement stmt = mySQLConn.createStatement();
        Iterator it = followersList.iterator();
        String idList = "";
        if (it.hasNext()) {
            idList = it.next().toString();
        }
        while (it.hasNext()) {
            idList += "' OR u_id='" + it.next().toString();
        }
        String sql = "SELECT name,url From userinfo WHERE u_id='" + idList + "' ORDER BY name,url";
        ResultSet rsSql = stmt.executeQuery(sql);
        while (rsSql.next()) {
            String name = rsSql.getString("name");
            String url = rsSql.getString("url");
            //If YES, send back the user's Name and Profile Image URL.
            JSONObject follower = new JSONObject();
            follower.put("name", name);
            follower.put("profile", url);
            followers.put(follower);
        }
        rsSql.close();
        return result.put("followers", followers);
    }

    public static JSONObject getPosts(String id, JSONObject result) throws IOException {
        //Get followee's posts
        List followeesList = new LinkedList<Document>();
        Scan scan = new Scan();
        byte[] bCol1 = Bytes.toBytes("followee");
        byte[] bCol2 = Bytes.toBytes("followers");
        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);
        BinaryComparator comp = new BinaryComparator(Bytes.toBytes(id));
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        scan.setBatch(10);
        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            String oneFollowee = Bytes.toString(r.getValue(bColFamily, bCol1));
            Document oneDoc = new Document("uid", Integer.parseInt(oneFollowee));
            followeesList.add(oneDoc);
        }
        rs.close();

        JSONArray posts = new JSONArray();
        final ArrayList<String> jsonList = new ArrayList<String>();
        FindIterable<Document> iterable = db.getCollection("userposts").find(new Document("$or", followeesList))
                .sort(new Document("timestamp", -1).append("pid", -1)).limit(30);   //this is descending order because later we will store it in reverse order
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                jsonList.add(0, document.toJson());
                //always put to the head of the array, not the end, so that it staisfies the requirement
            }
        });
        for (String post : jsonList) {
            JSONObject jsonPost = new JSONObject(post);
            posts.put(jsonPost);
        }
        return result.put("posts", posts);

    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
        try {
            getProfile(id, result);
            getFollowers(id, result);
            getPosts(id, result);
        } catch (Exception e) {
        }
        PrintWriter out = response.getWriter();
        //out.print(String.format("returnRes(%s)", result.toString()));
        out.write(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

}
