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
import static java.util.Arrays.asList;

import org.json.JSONObject;
import org.json.JSONArray;

public class HomepageServlet extends HttpServlet {

    private static MongoClient mongoClient;
    private static MongoDatabase db;

    public HomepageServlet() {
        //Initialize MongoDB
        mongoClient = new MongoClient("172.31.10.25", 27017);
        db = mongoClient.getDatabase("project34");
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        final JSONArray posts = new JSONArray();

        //Form the query
        FindIterable<Document> iterable = db.getCollection("userposts").find(new Document("uid", Integer.parseInt(id)))
                .sort(new Document("timestamp", 1));
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                //Iterate the result set and put the each result into the json array
                JSONObject post = new JSONObject(document.toJson());
                posts.put(post);
            }
        });
        result.put("posts", posts);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}
