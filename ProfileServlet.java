package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project34";
    private static final String URL = "jdbc:mysql://project3-4.cchoqihkcvdv.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;

    private static final String DB_USER = "wenhoulu";
    private static final String DB_PWD = "lwhlwh324324";
    private static Connection conn;

    public ProfileServlet() {
        //Initialize the MySQL connection
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ProfileServlet.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(ProfileServlet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        try {
            JSONObject result = new JSONObject();

            String id = request.getParameter("id");
            String pwd = request.getParameter("pwd");

            Statement stmt = conn.createStatement();
            String name = "Unauthorized";   //default value
            String url = "#";   //default value
            
            //Execute the query in MySQL
            String sql = "SELECT * From users,userinfo WHERE users.u_id = '" + id
                    + "' AND users.pwd = '" + pwd + "' AND users.u_id = userinfo.u_id";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                name = rs.getString("userinfo.name");
                url = rs.getString("userinfo.url");
                //If YES, send back the user's Name and Profile Image URL.
            }
            //If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
            result.put("name", name);
            result.put("profile", url);

            PrintWriter writer = response.getWriter();
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
        } catch (SQLException ex) {
            Logger.getLogger(ProfileServlet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
}
