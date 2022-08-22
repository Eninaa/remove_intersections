import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws IOException, SQLException, ParseException {


        File file = new File("test.geojson");
        file.createNewFile();
        Writer wr = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);

        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.57.102/test", "postgres", "samis");
        MongoClient mongo = MongoClients.create("mongodb://192.168.57.102:27017");

        MongoDatabase db = mongo.getDatabase("region63_samarskaya_obl");
        MongoCollection<Document> hresults = db.getCollection("h_results_test");

        MongoCursor<Document> hresCursor = hresults.find().iterator();
        MongoCursor<Document> res = null;


        JSONObject featureCollection = new JSONObject();
        JSONArray features = new JSONArray();
        featureCollection.put("type", "FeatureCollection");
        Instant start = Instant.now();

        while (hresCursor.hasNext()) {
            Document doc1 = hresCursor.next();
            Document geo1 = (Document) doc1.get("Geometry");
            String geom1;
            String geom2;
            ObjectId id1 = (ObjectId) doc1.get("_id");
            if (geo1 != null) {
                JSONObject geo = new JSONObject();
                String type = (String) geo1.get("type");
                ArrayList coors1 = (ArrayList) geo1.get("coordinates");
                geo.put("type", type);
                geo.put("coordinates", coors1);
                geom1 = geo.toString();

                geo1.toBsonDocument();
                res = hresults.find(Filters.geoIntersects("Geometry", geo1)).iterator();
                ObjectId id2;

                 while (res.hasNext()) {
                    Document doc2 = res.next();
                    Document geo2 = (Document) doc2.get("Geometry");

                    geo = new JSONObject();
                    type = (String) geo2.get("type");
                    ArrayList coors2 = (ArrayList) geo2.get("coordinates");
                    geo.put("type", type);
                    geo.put("coordinates", coors2);
                    geom2 = geo.toString();

                    id2 = (ObjectId) doc2.get("_id");
                    if (!id1.equals(id2)) {
                        String query = "{call rk_remove_intersections(?,?)}";
                        CallableStatement ps = conn.prepareCall(query);
                        ps.setString(1, geom1);
                        ps.setString(2, geom2);
                        ResultSet rs = ps.executeQuery();

                        JSONObject g1;
                        JSONObject g2;

                        JSONParser parser = new JSONParser();

                        JSONObject feature = new JSONObject();
                        JSONObject properties = new JSONObject();

                        while (rs.next()) {
                            g1 = (JSONObject) parser.parse(rs.getString(1));
                            g2 = (JSONObject) parser.parse(rs.getString(2));

                            /*BasicDBObject obj1 = new BasicDBObject();
                            BasicDBObject obj2 = new BasicDBObject();

                            obj1.put("_id", id1);
                            obj2.put("_id", id2);

                            JSONObject geometry1 = new JSONObject();
                            JSONObject geometry2 = new JSONObject();

                            geometry1.put("Geometry", g1);
                            geometry2.put("Geometry", g2);
                            BasicDBObject updateObj1 = new BasicDBObject();
                            BasicDBObject updateObj2 = new BasicDBObject();

                            updateObj1.put("$set", geometry1);
                            updateObj2.put("$set", geometry2);
                            hresults.updateOne(obj1, updateObj1);
                            hresults.updateOne(obj2, updateObj2);*/


                            feature.put("type", "Feature");
                            feature.put("geometry", g1);
                            feature.put("properties", properties);
                            features.add(feature);
                            feature = new JSONObject();
                            properties = new JSONObject();
                            feature.put("type", "Feature");
                            feature.put("geometry", g2);
                            feature.put("properties", properties);

                            features.add(feature);
                        }
                    }
                }
            }
        }

        featureCollection.put("features", features);
        wr.write(featureCollection.toJSONString());
        wr.close();
        mongo.close();
        conn.close();
        Instant finish = Instant.now();
        long elapsed = Duration.between(start, finish).toSeconds();
        System.out.println("Прошло времени, с: " + elapsed);
    }
}
