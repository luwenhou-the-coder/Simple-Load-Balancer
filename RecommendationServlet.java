package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import org.json.JSONObject;
import org.json.JSONArray;

public class RecommendationServlet extends HttpServlet {

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

    public RecommendationServlet() throws Exception {
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

    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        try {
            JSONObject result = new JSONObject();   //final result as a JSON object
            String id = request.getParameter("id");

            Set<String> followeesList = new HashSet<String>();  //create a linked list to store followee's id
            Map<Long, Integer> followeesMap = new ConcurrentHashMap<Long, Integer>();
            //create a map to store followee's id as key and score as value

            //Execute HBase query to get all IDs of the followers
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
                //for each followee, do the query again to find followee whoes distance is 2
                String oneFollowee = Bytes.toString(r.getValue(bColFamily, bCol1));
                followeesList.add(oneFollowee);
            }
            rs.close();
            System.out.println(followeesList);

            Scan scan2 = new Scan();
            scan2.addColumn(bColFamily, bCol1);
            scan2.addColumn(bColFamily, bCol2);

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            for (String eachFo : followeesList) {
                BinaryComparator comp2 = new BinaryComparator(Bytes.toBytes(eachFo));
                filterList.addFilter(new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp2));
            }
            scan2.setFilter(filterList);
            scan2.setBatch(50);
            ResultScanner rs2 = table.getScanner(scan2);
            for (Result r2 = rs2.next(); r2 != null; r2 = rs2.next()) {
                String oneFollowee2 = Bytes.toString(r2.getValue(bColFamily, bCol1));
                if (!followeesList.contains(oneFollowee2)) {    //do not inclued followee whoes distance is 1
                    long longId = Long.parseLong(oneFollowee2);
                    if (followeesMap.containsKey(longId)) {
                        //calculate the score
                        int score = followeesMap.get(longId);
                        score++;
                        followeesMap.put(longId, score);
                    } else {
                        followeesMap.put(longId, 1);
                    }
                }
            }
            rs2.close();

            List<Map.Entry<Long, Integer>> list = new ArrayList<Map.Entry<Long, Integer>>(followeesMap.entrySet());
            Collections.sort(list, new mapComparator<Long, Integer>()); //sort the map based on required rules

            Iterator it = list.iterator();   //iterate the map and generate the query criteria

            int count = 0;
            //Execute the MySQL query to find the corresponding name and profile url
            JSONArray recommendations = new JSONArray();
            Statement stmt = mySQLConn.createStatement();

            while (it.hasNext()) {
                if (count >= 10) {  //limit the number of recommendations to 10
                    break;
                }
                Map.Entry pair = (Map.Entry) it.next();
                String queryId = pair.getKey().toString();
                String sql = "SELECT name,url From userinfo WHERE u_id='" + queryId + "'";
                ResultSet rsSql = stmt.executeQuery(sql);
                while (rsSql.next()) {
                    String name = rsSql.getString("name");
                    String url = rsSql.getString("url");
                    JSONObject recommendation = new JSONObject();
                    recommendation.put("name", name);
                    recommendation.put("profile", url);
                    recommendations.put(recommendation);
                }
                rsSql.close();
                it.remove();
                count++;
            }
            result.put("recommendation", recommendations);   //put into the final result
            System.out.println(result.toString());
            PrintWriter writer = response.getWriter();
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
        } catch (SQLException ex) {
        }

    }

    class mapComparator<K extends Comparable<? super K>, V extends Comparable<? super V>>
            implements Comparator<Map.Entry<K, V>> {

        public int compare(Map.Entry<K, V> a, Map.Entry<K, V> b) {
            int cmp1 = a.getValue().compareTo(b.getValue());
            if (cmp1 != 0) {
                return -cmp1;
            } else {
                return a.getKey().compareTo(b.getKey());
            }
        }

    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
}
