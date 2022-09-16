import com.mongodb.BasicDBObject;
import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Main {
    static JSONObject parseGeo(Document geo) {
        JSONObject geoObj = new JSONObject();
        JSONObject g = new JSONObject();
        if (geo != null) {
            String type = (String) geo.get("type");
            if (!type.equals("GeometryCollection")) {
                ArrayList coors = (ArrayList) geo.get("coordinates");
                geoObj.put("type", type);
                geoObj.put("coordinates", coors);
                g = geoObj;
            }
        }
        return g;
    }

    static void updateObject(MongoCollection collection, ObjectId id, JSONObject geom) throws MongoWriteException {
        BasicDBObject obj = new BasicDBObject();
        obj.put("_id", id);

        JSONObject geometry = new JSONObject();
        geometry.put("Geometry", geom);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", geometry);

        collection.updateOne(obj, updateObj);
    }

    static void makePolygons(MongoCollection collection, Connection conn) throws SQLException, ParseException {
        JSONParser parser = new JSONParser();
        String query = "{call rk_makePolygon(?)}";
        CallableStatement ps = conn.prepareCall(query);
        BasicDBObject filter = new BasicDBObject();
        filter.put("Geometry.type", "Point");

        MongoCursor<Document> cursor = collection.find(filter).iterator();
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            Document geo = (Document) doc.get("Geometry");
            ObjectId id = doc.getObjectId("_id");
            if (geo != null && !geo.isEmpty()) {
                JSONObject obj = parseGeo(geo);
                String geom = obj.toString();;
                if (geom != null) {
                    ps.setString(1, geom);
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        if (rs.getString(1) != null) {
                            JSONObject g = (JSONObject) parser.parse(rs.getString(1));
                            updateObject(collection, id, g);
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, SQLException, ParseException {
        try {
            LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logProperties"));
        } catch (IOException e) {
            System.err.println("Could not setup logger configuration: " + e.toString());
        }
        Logger log = Logger.getLogger(Main.class.getName());

        JSONParser parser = new JSONParser();

        ObjectId id1 = null;
        ObjectId id2 = null;
        try {
            String ConnectionStringMongo;
            String URL;
            String user;
            String password;
            String collection;
            double bufferSize;
            long iterationsCount;
            double simplifyPrecision;
            double loopPrecision;
            double sliverArea;

            Instant start = Instant.now();

            Reader reader = new InputStreamReader(new FileInputStream("settings.json"), StandardCharsets.UTF_8);
            JSONObject obj = (JSONObject) parser.parse(reader);
            ConnectionStringMongo = (String) obj.get("ConnectionStringMongo");
            JSONObject ConnectionStringPostgre = (JSONObject) obj.get("ConnectionStringPostgre");
            URL = (String) ConnectionStringPostgre.get("URL");
            user = (String) ConnectionStringPostgre.get("user");
            password = (String) ConnectionStringPostgre.get("password");
            collection = (String) obj.get("collection");

            bufferSize = (double) obj.get("bufferSize");
            iterationsCount = (long) obj.get("iterationsCount");
            simplifyPrecision = (double) obj.get("simplifyPrecision");
            loopPrecision = (double) obj.get("loopPrecision");
            sliverArea = (double) obj.get("sliverArea");

            Connection conn = DriverManager.getConnection(URL, user, password);

            String query = "{call rk_removeIntersections(?, ?, ?, ?, ?, ?, ?)}";
            CallableStatement ps = conn.prepareCall(query);
            ps.setDouble(3, bufferSize);
            ps.setLong(4, iterationsCount);
            ps.setDouble(5, simplifyPrecision);
            ps.setDouble(6, loopPrecision);
            ps.setDouble(7, sliverArea);

            MongoClient mongo = MongoClients.create(ConnectionStringMongo);
            MongoDatabase db = mongo.getDatabase(args[0]);
            MongoCollection<Document> dbCollection = db.getCollection(collection);
            makePolygons(dbCollection, conn);

            MongoCursor<Document> hresCursor = dbCollection.find().iterator();
            MongoCursor<Document> intersections;

            HashMap<ObjectId, JSONObject> updates = new HashMap<>();

            while (hresCursor.hasNext()) {
                Document doc1 = hresCursor.next();
                id1 = doc1.getObjectId("_id");
                Document geoDoc1 = (Document) doc1.get("Geometry");
                if (geoDoc1 != null) {
                    intersections = dbCollection.find(Filters.geoIntersects("Geometry", geoDoc1.toBsonDocument())).iterator();
                    while (intersections.hasNext()) {
                        String geom1 = null;
                        String geom2 = null;
                        Document doc2 = intersections.next();
                        id2 = doc2.getObjectId("_id");
                        Document geoDoc2 = (Document) doc2.get("Geometry");
                        if (!(id1.equals(id2))) {
                            if (updates.containsKey(id1)) {
                                geom1 = updates.get(id1).toString();
                            } else {
                                geom1 = parseGeo(geoDoc1).toString();
                            }
                            if (updates.containsKey(id2)) {
                                geom2 = updates.get(id2).toString();
                            } else {
                                geom2 = parseGeo(geoDoc2).toString();
                            }
                            if (geom1 != null && geom2 != null) {
                                ps.setString(1, geom1);
                                ps.setString(2, geom2);
                                ResultSet rs = ps.executeQuery();
                                while (rs.next()) {
                                    if (rs.getString(1) != null && rs.getString(2) != null) {
                                        JSONObject g1 = (JSONObject) parser.parse(rs.getString(1));
                                        JSONObject g2 = (JSONObject) parser.parse(rs.getString(2));
                                        updates.put(id1, g1);
                                        updates.put(id2, g2);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            int unupdated = 0;
            for (Map.Entry entry : updates.entrySet()) {
                JSONObject g = (JSONObject) entry.getValue();
                ObjectId id = (ObjectId) entry.getKey();
                try {
                    updateObject(dbCollection, id, g);
                } catch (MongoWriteException e) {
                    unupdated++;
                    log.log(Level.SEVERE, "Object " + id + " wasn't updated!" + e);
                }
            }
            Instant finish = Instant.now();
            mongo.close();
            conn.close();
            long elapsed = Duration.between(start, finish).toSeconds();
            log.log(Level.SEVERE, "Unupdated objects count: " + unupdated);
            log.log(Level.SEVERE, "Duration, seconds: " + elapsed);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Some error occurred! Last objects: " + id1 + " " + id2 + " " + e);
        }
    }
}
