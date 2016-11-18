package com.capiot.streambase;

import com.capiot.streambase.mongoUtil.SharedMongoClient;
import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Tuple;
import com.streambase.sb.operator.Operator;
import com.streambase.sb.operator.Parameterizable;
import com.streambase.sb.operator.TypecheckException;
import org.bson.Document;
import org.slf4j.Logger;

/**
 * Generated by JDT StreamBase Client Templates (Version: 7.6.3.1604271124).
 * <p>
 * This class is used as a Java Operator in a StreamBase application.
 * One instance will be created for each Java Operator in a StreamBase
 * application.
 * <p>
 * Enqueue methods should only be called from processTuple.
 * <p>
 * If there is any state that the operator instance needs to maintain beyond the
 * lifetime of the instance, the setSessionState/getSessionState methods can
 * be used.
 *
 * @see Parameterizable
 * @see Operator
 * For in-depth information on implementing a custom Java Operator, please see
 * "Developing StreamBase Java Operators" in the StreamBase documentation.
 */
public class Mongo extends Operator implements Parameterizable {

    public static final long serialVersionUID = 1470151741656L;
    private Logger logger;
    // Properties
    private Schema InsertSchema;
    private String Url;
    private String collection;
    private String DB;
    private boolean PurgeOnConnect;

    private String displayName = "Mongo";
    // Local variables
    private int inputPorts = 1;
    private int outputPorts = 1;
    private Schema[] outputSchemas; // caches the Schemas given during init() for use at processTuple()
    private MongoCore mCore = null;

    private boolean sharedClient;

    /**
     * The constructor is called when the Operator instance is created, but before the Operator
     * is connected to the StreamBase application. We recommended that you set the initial input
     * port and output port count in the constructor by calling setPortHints(inPortCount, outPortCount).
     * The default is 1 input port, 1 output port. The constructor may also set default values for
     * operator parameters. These values will be displayed in StreamBase Studio when a new instance
     * of this operator is  dragged to the canvas, and serve as the default values for omitted
     * optional parameters.
     */
    public Mongo() {
        super();
        logger = getLogger();
        setPortHints(inputPorts, outputPorts);
        setDisplayName(displayName);
        setShortDisplayName(this.getClass().getSimpleName());
        setInsertSchema(null);
        setUrl("mongodb://localhost:27017");
        setCollection("default");
        setDB("default");
        setPurgeOnConnect(false);
    }

    /**
     * The typecheck method is called after the Operator instance is connected in the StreamBase
     * application, allowing the Operator to validate its properties. The Operator class may
     * change the number of input or output ports by calling the requireInputPortCount(portCount)
     * method or the setOutputSchema(schema, portNum) method. If the verifyInputPortCount method
     * is passed a different number of ports than the Operator currently has, a PortMismatchException
     * (subtype of TypecheckException) is thrown.
     */
    public void typecheck() throws TypecheckException {
        // typecheck: require a specific number of input ports
        //requireInputPortCount(inputPorts);

        Schema read = getInputSchema(0);
        if (read != null) {
            if (!read.hasField("filter")) {
                throw new TypecheckException("Read Input does not contain filter / ID");
            } else if (!read.hasField("Collection")) {
                throw new TypecheckException("Missing field\"Collection\"");
            } else if (!read.hasField("Command")) {
                throw new TypecheckException("Missing field\"Command\"");
            } else if (!read.hasField("filter")) {
                throw new TypecheckException("Missing field \"filter\"");
            }
        }


        setOutputSchema(0, MongoCore.getSchema());
    }

    private void generateAndRespond(String ID, String data) {
        Tuple t = MongoCore.getSchema().createTuple();
        try {
            t.setString("ID", ID);
            t.setString("Data", data);
            sendOutputAsync(0, t);
        } catch (StreamBaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return;
    }

    private void runTimeCheck(Tuple tuple, String[] types) throws StreamBaseException {
        for (String type : types) {
            if (tuple.isNull(type)) {
                throw new StreamBaseException("Expected " + type + " to be a non-null value");
            }
        }
    }

    /**
     * This method will be called by the StreamBase server for each Tuple given
     * to the Operator to process. This is the only time an operator should
     * enqueue output Tuples.
     *
     * @param inputPort the input port that the tuple is from (ports are zero based)
     * @param tuple     the tuple from the given input port
     * @throws StreamBaseException Terminates the application.
     */
    public void processTuple(int inputPort, final Tuple tuple)
            throws StreamBaseException {
        runTimeCheck(tuple, new String[]{"ID", "Command"});
        final String ID = tuple.getString("ID");
        final String cmd = tuple.getString("Command").toLowerCase();

        if (cmd == "insert") {
            runTimeCheck(tuple, new String[]{"Collection", "Data"});
            mCore.insertData(tuple.getString("Collection"), tuple.getString("Data"), new SingleResultCallback<Document>() {

                @Override
                public void onResult(Document arg0, Throwable arg1) {
                    // TODO Auto-generated method stub
                    generateAndRespond(ID, arg0.toJson());
                }
            });
        } else if (cmd == "read") {
            runTimeCheck(tuple, new String[]{"Collection", "filter"});
            mCore.getData(tuple.getString("Collection"), tuple.getString("filter"), new Block<Document>() {
                @Override
                public void apply(Document arg0) {
                    generateAndRespond(ID, arg0.toJson());
                }
            });
        } else if (cmd == "update") {
            runTimeCheck(tuple, new String[]{"filter", "Data"});
            mCore.updateData(tuple.getString("Collection"), tuple.getString("filter"), tuple.getString("Data"), new SingleResultCallback<Document>() {

                @Override
                public void onResult(final Document arg0, Throwable arg1) {
                    // TODO Auto-generated method stub
                    String result = "";
                    if (arg0 != null) {
                        result = arg0.toJson();
                    }
                    generateAndRespond(ID, result);
                }
            });
        } else if (cmd == "findoneanddelete") {
            runTimeCheck(tuple, new String[]{"Collection", "filter", "Data"});
            mCore.findOneAndUpdate(tuple.getString("Collection"), tuple.getString("filter"), tuple.getString("Data"), (result, t) -> {
                generateAndRespond(ID, result.toJson());
            });
        } else if (cmd == "delete") {
            runTimeCheck(tuple, new String[]{"Collection", "_id"});
            mCore.deleteData(tuple.getString("Collection"), tuple.getString("_id"), new SingleResultCallback<Document>() {

                @Override
                public void onResult(Document arg0, Throwable arg1) {
                    // TODO Auto-generated method stub
                    String ret = "";
                    if (arg0 != null) {
                        ret = arg0.toJson();
                    } else if (arg1 != null) {
                        System.out.println("Error : " + arg1.getMessage());
                    }
                    generateAndRespond(ID, ret);
                }

            });
        }
    }
//	@Override
//	public java.net.URL getIconResource(IconKind iconType) {
//		URL url = null;
//		try {
//			url = new File("resources/mongodb.jpg").toURI().toURL();
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("Called URL!" + url.getPath());
//		return url;
//	};

    /**
     * If typecheck succeeds, the init method is called before the StreamBase application
     * is started. Note that your Operator class is not required to define the init method,
     * unless (for example) you need to perform initialization of a resource such as a JDBC
     * pool, if your operator is making JDBC calls. StreamBase Studio does not call this
     * during authoring.
     */
    public void init() throws StreamBaseException {
        super.init();
        // for best performance, consider caching input or output Schema.Field objects for
        // use later in processTuple()
        outputSchemas = new Schema[outputPorts];
        MongoClient c = null;
        if (sharedClient) {
            SharedObjectManager shom = this.getRuntimeEnvironment().getSharedObjectManager();
            SharedMongoClient shc = (SharedMongoClient) shom.getSharedObject(Url);
            if (shc == null) {
                c = MongoClients.create(getUrl());
                shc = new SharedMongoClient(getUrl());
                shom.registerSharedObject(getUrl(), shc);
                shc.startObject(); //Connect to the DB.
            }
            c = shc.getClient();
        } else {
            c = MongoClients.create(getUrl());
        }
        for (int i = 0; i < outputPorts; ++i) {
            outputSchemas[i] = getRuntimeOutputSchema(i);
        }
        mCore = new MongoCore(c, collection, DB);
    }

    /**
     * The shutdown method is called when the StreamBase server is in the process of shutting down.
     */
    public void shutdown() {

    }

    public Schema getInsertSchema() {
        return this.InsertSchema;
    }

    /***************************************************************************************
     * The getter and setter methods provided by the Parameterizable object.               *
     * StreamBase Studio uses them to determine the name and type of each property         *
     * and obviously, to set and get the property values.                                  *
     ***************************************************************************************/

    public void setInsertSchema(Schema InsertSchema) {
        this.InsertSchema = InsertSchema;
    }

    public String getUrl() {
        return this.Url;
    }

    public void setUrl(String Url) {
        this.Url = Url;
    }

    public String getCollection() {
        return this.collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
        if (mCore != null) {
            mCore.setCollection(collection, PurgeOnConnect);
        }
    }

    /**
     * For detailed information about shouldEnable methods, see interface Parameterizable java doc
     *
     * @see Parameterizable
     */

    public boolean shouldEnableInsertSchema() {
        // TODO implement custom enablement logic here
        return true;
    }

    public boolean shouldEnableUrl() {
        // TODO implement custom enablement logic here
        return true;
    }

    public boolean shouldEnableCollection() {
        // TODO implement custom enablement logic here
        return true;
    }

    /**
     * @return the dB
     */
    public String getDB() {
        return DB;
    }

    /**
     * @param dB the dB to set
     */
    public void setDB(String dB) {
        DB = dB;
    }

    /**
     * @return the purgeOnConnect
     */
    public boolean getPurgeOnConnect() {
        return PurgeOnConnect;
    }

    /**
     * @param purgeOnConnect the purgeOnConnect to set
     */
    public void setPurgeOnConnect(boolean purgeOnConnect) {
        this.PurgeOnConnect = purgeOnConnect;
    }

    public boolean isSharedClient() {
        return sharedClient;
    }

    public void setSharedClient(boolean sharedClient) {
        this.sharedClient = sharedClient;
    }


}
