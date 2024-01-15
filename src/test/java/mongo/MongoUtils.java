package mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoUtils {

    public static final String MONGO_URI = "mongodb://demo:demo@localhost:27017/odm-ng?authSource=admin&connectTimeoutMS=3000";

    public static MongoClient createMongoClient() {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO_URI)).build());
    }
}
