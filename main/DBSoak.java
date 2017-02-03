package uk.gov.ons.dp.mongo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.bulk.BulkWriteResult;
import org.bson.Document;
import com.mongodb.event.CommandListener;
import com.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.List;
import java.net.UnknownHostException;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.Duration;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;

public class DBSoak {

    public static void main(String[] args) throws UnknownHostException {

        int maxDocs   = 10;
        int batchSize = 5;
        int showPer   = 100000;
        String doing  = "inserts";

        int insertCount = 0;
        int updateCount = 0;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("inserts") || args[i].equals("upserts") || args[i].equals("replaces")) {
                doing = args[i];
            } else {
                int num = Integer.parseInt(args[i]);
                if (num > 1000) {
                    maxDocs = num;
                } else {
                    batchSize = num;
                }
            }
        }
        if (showPer > maxDocs/10) {
            showPer = maxDocs/10;
        }

        LocalDateTime startTime = LocalDateTime.now();
        MongoClient mongo = new MongoClient();
        MongoDatabase db = mongo.getDatabase("dbsoak");
        MongoCollection<Document> col = db.getCollection("java");

        List<Document> documents = new ArrayList<Document>();       // inserts
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();  // upserts replaces

        showWork(doing, 0, maxDocs, batchSize, startTime, insertCount, updateCount);

        for (int i = 0; i < maxDocs; i++ ) {

            int myid = i % 1000;

            Document doc = null;
            if (doing.equals("inserts") || doing.equals("replaces")) {
                doc = createDBObject(myid, i, new java.sql.Timestamp(new Date().getTime()));
                // new Document("_id", new ObjectId(id))
            }

            if (batchSize == 0) {
                if (doing.equals("inserts")) {
                    col.insertOne(doc);
                } else if (doing.equals("upserts")) {
                    col.updateOne(eq("myid", myid), createDBUpdate(myid, i, new java.sql.Timestamp(new Date().getTime())));
                } else if (doing.equals("replaces")) {
                    col.replaceOne(eq("myid", myid), doc);
                }
                // System.out.println(result.getUpsertedId()); System.out.println(result.getN()); System.out.println(result.isUpdateOfExisting()); System.out.println(result.getLastConcern());

            } else if (batchSize > 0) {
                if (doing.equals("inserts")) {
                    // documents.add(doc);
                    writes.add(new InsertOneModel<Document>(doc));
                } else if (doing.equals("upserts")) {
                    writes.add(
                        new UpdateOneModel<Document>(
                            new Document("myid", myid),
                            createDBUpdate(myid, i, new java.sql.Timestamp(new Date().getTime())),
                            new UpdateOptions().upsert(true)
                        )
                    );
                } else if (doing.equals("replaces")) {
                    writes.add(
                        new ReplaceOneModel<Document>(
                            new Document("myid", myid),
                            doc,
                            new UpdateOptions().upsert(true)
                        )
                    );
                }

                if (batchSize > 0 && writes.size() >= batchSize) {
                    BulkWriteResult bulkRes = col.bulkWrite(writes);
                    insertCount += bulkRes.getInsertedCount();
                    updateCount += bulkRes.getModifiedCount();
                    writes = new ArrayList<WriteModel<Document>>();     // reset
                }
                if ((i+1) % showPer == 0) {
                    showWork(doing, i+1, maxDocs, batchSize, startTime, insertCount, updateCount);
                }
            } else {
            }
        }

        if (batchSize > 0 && writes.size() > 0) {
            BulkWriteResult bulkRes = col.bulkWrite(writes);
            insertCount += bulkRes.getInsertedCount();
            updateCount += bulkRes.getModifiedCount();
        }
        showWork(doing, maxDocs, maxDocs, batchSize, startTime, insertCount, updateCount);
        mongo.close();
    }

    private static Document createDBObject(int myid, int myval, Date timestamp) {
        return new Document("myid", myid).append("myval", myval).append("timestamp", timestamp);
    }

    private static Document createDBUpdate(int myid, int myval, Date timestamp) {
        return new Document("$set", new Document("myval", myval).append("timestamp", timestamp));
    }

    private static void showWork(String doing, int done, int maxDocs, int batchSize, LocalDateTime startTime, int insertCount, int updateCount) {
        LocalDateTime now = LocalDateTime.now();
        Duration delta = Duration.between(startTime, now);
        long mSecsNonZero = delta.toMillis();
        if (mSecsNonZero == 0) { mSecsNonZero = 1; }
        System.out.printf("%s java %s %d / %d (%d per batch) in %s = %.1f/s  ins:%d ups:%d\n",
                now.toString(), doing, done, maxDocs, batchSize, delta, (float)(done*1000/mSecsNonZero), insertCount, updateCount);
    }

}
