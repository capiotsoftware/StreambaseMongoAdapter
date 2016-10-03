package com.capiot.streambase.mongoUtil;

import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.operator.Operator;

/**
 * Created by abilashmohan on 03/10/16.
 */
public class SharedMongoClient implements Operator.SharedObject {
    private MongoClient client = null;

    private String uri;

    public SharedMongoClient(String uri) {
        this.uri = uri;
    }

    private void connect() {
        if (client == null) {
            client = MongoClients.create(uri);
        }
    }

    private void disconnect() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void startObject() throws StreamBaseException {
        this.connect();
    }

    @Override
    public void resumeObject() throws StreamBaseException {
        this.connect();
    }

    @Override
    public void suspendObject() throws StreamBaseException {

    }

    @Override
    public void shutdownObject() throws StreamBaseException {
        this.disconnect();
    }

    public MongoClient getClient() {
        return client;
    }
}
