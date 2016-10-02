package com.capiot.streambase;


import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import org.bson.Document;
import org.bson.types.ObjectId;

public class MongoCore {
    private static Schema schema;

    static {
        schema = new Schema("MongoAdapter", new Schema.Field[]{
                Schema.createField(DataType.STRING, "ID"),
                Schema.createField(DataType.STRING, "Collection"),
                Schema.createField(DataType.STRING, "Data")
        });
    }

    private MongoCollection<Document> collection;
    private MongoDatabase db;

    public MongoCore(MongoClient client, String DB) {
        this.db = client.getDatabase(DB);
    }

    public MongoCore(MongoClient client, String Collection, String DB) {
        this.db = client.getDatabase(DB);
        this.collection = this.db.getCollection(Collection);
    }

    ;

    /**
     * @return the schema
     */
    public static Schema getSchema() {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public static void setSchema(Schema schema) {
        MongoCore.schema = schema;
    }

    public void setCollection(String collection, boolean purgeOnConnect) {
        this.collection = this.db.getCollection(collection);

        if (purgeOnConnect)
            this.collection.deleteMany(Document.parse("{}"), new SingleResultCallback<DeleteResult>() {

                @Override
                public void onResult(DeleteResult arg0, Throwable arg1) {
                    // TODO Auto-generated method stub
                    System.out.println("Deleted All documents...");
                }

            });
    }

    public void getData(String collection, String filter, Block<Document> callback) {
        Document bFilter = null;
        if (filter != null) {
            bFilter = Document.parse(filter);
        } else {
            bFilter = Document.parse("{}");
        }
        db.getCollection(collection).find(bFilter).forEach(callback, new SingleResultCallback<Void>() {

            @Override
            public void onResult(Void arg0, Throwable arg1) {
                // TODO Auto-generated method stub
                return;
            }
        });
    }

    public void insertData(String collection, String data, final SingleResultCallback<Document> callback) {
        final Document payload = Document.parse(data);
        db.getCollection(collection).insertOne(payload, new SingleResultCallback<Void>() {

            @Override
            public void onResult(Void arg0, Throwable arg1) {
                // TODO Auto-generated method stub
                callback.onResult(payload, arg1);
            }
        });
    }

    public void updateData(String collection, String _id, String data, final SingleResultCallback<Document> callback) {
        final Document payload = Document.parse(data);
        final ObjectId oid = new ObjectId(_id);
        final Document filter = new Document();
        filter.append("_id", oid);
        final Document set = new Document();
        set.append("$set", payload);
        db.getCollection(collection).findOneAndUpdate(filter, set, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER), new SingleResultCallback<Document>() {

            @Override
            public void onResult(Document arg0, Throwable arg1) {
                // TODO Auto-generated method stub
                if (arg1 != null) {
                    System.out.println("arg1" + arg1.getMessage());
                }
                //payload.append("_modified", arg0.getModifiedCount());
                callback.onResult(arg0, arg1);
            }
        });
    }

    public void deleteData(String Collection, String _id, SingleResultCallback<Document> callback) {
        Document filter = new Document();
        filter.append("_id", new ObjectId(_id));
        System.out.println("Filter : " + filter.toJson());
        db.getCollection(Collection).findOneAndDelete(filter, callback);
    }

}
