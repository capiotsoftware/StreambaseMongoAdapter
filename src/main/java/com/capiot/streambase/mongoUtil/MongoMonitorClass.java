package com.capiot.streambase.mongoUtil;

import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;

import java.util.concurrent.TimeUnit;

/**
 * Created by abilashmohan on 20/11/16.
 */
public class MongoMonitorClass implements ServerMonitorListener {
    private static Schema schema;

    static {
        schema = new Schema("MongoMonitor", Schema.createField(DataType.STRING, "Server"),
                Schema.createField(DataType.STRING, "Event"),
                Schema.createField(DataType.LONG, "Elapsed"),
                Schema.createField(DataType.STRING, "Error"));
    }

    private sendable eventHandler = null;

    public MongoMonitorClass(sendable eventHandler) {
        this.eventHandler = eventHandler;
    }

    static public Schema getSchema() {
        return schema;
    }

    @Override
    public void serverHearbeatStarted(ServerHeartbeatStartedEvent serverHeartbeatStartedEvent) {
        serverEvent(serverHeartbeatStartedEvent.getConnectionId().toString(), "Heartbeat Started", null, null);
    }

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent serverHeartbeatSucceededEvent) {
        serverEvent(serverHeartbeatSucceededEvent.getConnectionId().toString(), "Heartbeat OK",
                serverHeartbeatSucceededEvent.getElapsedTime(TimeUnit.SECONDS), null);

    }

    @Override
    public void serverHeartbeatFailed(ServerHeartbeatFailedEvent serverHeartbeatFailedEvent) {
        serverEvent(serverHeartbeatFailedEvent.getConnectionId().toString(), "Heartbeat Failed",
                serverHeartbeatFailedEvent.getElapsedTime(TimeUnit.SECONDS),
                serverHeartbeatFailedEvent.getThrowable().getMessage());
    }

    public void serverEvent(String serverName, String message, Long lapsedTime, String error) {
        Tuple toSend = schema.createTuple();
        try {
            if (serverName != null) {
                toSend.setString("Server", serverName);
            }
            if (message != null) {
                toSend.setString("Event", message);
            }
            if (lapsedTime != null) {
                toSend.setLong("Elapsed", lapsedTime);
            }
            if (error != null) {
                toSend.setString("Error", error);
            }
            eventHandler.send(toSend);
        } catch (TupleException e) {
            e.printStackTrace();
        }
    }


    public interface sendable {
        void send(Tuple tuple);
    }
}
