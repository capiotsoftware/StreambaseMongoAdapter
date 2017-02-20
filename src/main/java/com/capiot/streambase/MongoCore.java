package com.capiot.streambase;


import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSONParseException;
import com.streambase.org.apache.commons.lang3.ArrayUtils;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseRuntimeException;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MongoCore {
    private static Schema schema;

    static {
        schema = new Schema("MongoAdapter", Schema.createField(DataType.STRING, "ID"),
                Schema.createField(DataType.STRING, "Collection"),
                Schema.createField(DataType.STRING, "Data"),
                Schema.createField(DataType.BOOL, "Error"),
                Schema.createField(DataType.STRING, "ErrorMessage"));
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


    public void getData(String collection, String filter, SingleResultCallback<Document> callback) {
        Document bFilter = null;
        if (filter != null) {
            bFilter = Document.parse(filter);
        } else {
            bFilter = Document.parse("{}");
        }
        db.getCollection(collection).find(bFilter).forEach(
                (document) -> callback.onResult(document, null),
                (end, err) -> {
                    if (err != null) {
                        callback.onResult(null, err);
                    } else {
                        Document doc = new Document("$$EOF", true);
                        callback.onResult(doc, null);
                    }
                }
        );
    }

    public void insertData(String collection, String data, final SingleResultCallback<Document> callback) {
        final Document payload = Document.parse(data);
        db.getCollection(collection).insertOne(payload, (Void arg0, Throwable arg1) ->
                callback.onResult(payload, arg1));
    }

    public void bulkinsert(String collection, String[] data, final SingleResultCallback<Document> callback){
        try {
            final List<InsertOneModel<Document>> payload = Arrays
                    .stream(data)
                    .map(e -> new InsertOneModel<>(Document.parse(e)))
                    .collect(Collectors.toList());
            db.getCollection(collection).bulkWrite(payload,(result,t) -> {
                Document doc = null;
                if(result != null){
                    doc = new Document();
                    doc.append("nInserted", result.getInsertedCount());
                    doc.append("wasAcknowledged",result.wasAcknowledged());
                }
                callback.onResult(doc,t);
            });
        }
        catch(Exception e){
            callback.onResult(new Document(),e);
        }

    }

    public void updateData(String collection, String filter,boolean upsert, String data, final SingleResultCallback<Document> callback) {
        try {
            final Document payload = Document.parse(data);
            final Document selector = Document.parse(filter);
            //        final Document set = new Document();
            //        set.append("$set", payload);
            UpdateOptions upd = new UpdateOptions();
            upd.upsert(upsert);
            db.getCollection(collection).updateMany(selector, payload,upd, (UpdateResult result, Throwable t) -> {
                    Document doc = new Document();
                    doc.append("nModified", (int) result.getModifiedCount());
                    doc.append("nMatched", (int) result.getMatchedCount());
                    doc.append("acknowledged", result.wasAcknowledged());
                    doc.append("error", t != null);
                    doc.append("errorMessage", t == null ? "" : t.getMessage());
                    callback.onResult(doc, t);
            });
        } catch (JSONParseException e) {
            throw new StreamBaseRuntimeException(e.getMessage());
        }
    }

    public void findOneAndUpdate(String collection, String filter, String data, SingleResultCallback<Document> callback) {
        try {
            final Document payload = Document.parse(data);
            final Document selector = Document.parse(filter);
            db.getCollection(collection).findOneAndUpdate(selector, payload, callback);
        } catch (JSONParseException e) {
            throw new StreamBaseRuntimeException(e.getMessage());
        }
    }

    public void deleteData(String Collection, String filter, SingleResultCallback<Document> callback) {
        try {
            Document _filter = Document.parse(filter);
            db.getCollection(Collection).deleteMany(_filter, (result, t) -> {
                Document ret = new Document();
                ret.append("nDelted", (int) result.getDeletedCount());
                ret.append("acknowledged", result.wasAcknowledged());
                ret.append("error", t != null);
                ret.append("errorMessage", t != null ? t.getMessage() : "");
                callback.onResult(ret, t);
            });
        } catch (JSONParseException e) {
            throw new StreamBaseRuntimeException(e.getMessage());
        }
    }

}
