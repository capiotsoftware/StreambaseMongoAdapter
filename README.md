# MongoDB Operator for Streambase
## Features
* Built with MongoDB high performance asynchronous driver.
* Ability to share the driver with multiple operators. 
* Ability switch collections on each write.
* Works by taking and producing JSON data. Use JSON->Tuple and Tuple->JSON converters.

*Note :*
* Adapter designed to work with Streambase version 7.6.5 (Previous builds might work). 
* Desiged to work with MongoDB Java driver Version 3.3.0


## Build Instructions
* This is an IntelliJ Project. Please use the same. 
* Open the pom.xml and update the <url> of SB76-local-repo to the streambase installation on your machine.
* Once done, issue the following instruction to create a single Jar that can be used as a drop in for Streambase

    `mvn clean compile assembly:single`

## Operator usage Instructions
MongoDB operator usage is pretty simple. 
Under "Operator Properties" we have the following operations.
### Operator Properties
#### Mongo URL
Accepts the full fledge Mongo URI naming scheme. Please read [Mongo Connection String URI](https://docs.mongodb.com/v3.0/reference/connection-string/)

#### Mongo DB
The DB to which Mongo Operator will connect to. The value will be read at initialization phase. Any changes post initialization is ignored.

#### Collection Name
Please ignore, not used anymore. Will be removed in a future release.

#### Shared Client
Enabling this option will cause operators with same Mongo URL to share a single MongoDB Client.

#### Monitor Connections
Monitor Connection adds an additional output port to Mongo DB. This port will contain the heartbeat exchange information between the client and the server. 

### Operator input instructions
Operator input channel expects following tuples in the input : 
* ID ( String )
* Filter ( String )
* Collection ( String )
* Data ( String )
* Command ( String )

#### ID
Since the Mongo driver is asynchronous, you need to use the ID as a correlation between what goes in and comes out of the operator.
Please note : The driver does not enforce any rules on ID, it is not persisted either. The values are simply forwarded back.

#### Filter
This field is used for filtering the data in case of an update, read or delete.

#### Data
This field holds the payload (and operation on the document) that needs to tbe persisted. Applicable for update.

#### Command
Command can be one of the following
* Insert - Insert a document within a collection.
* Read - Read one or more entries from MongoDB Collection, depending on what is supplied to "Filter". There will be a '{"$$EOF" : true }' in Data once the operator exhausts all the documents for a read operation.
* Update - Update one or more documents in the collection, depending on what is supplied at "Filter" and "Data"
* Upsert - Update the document if it exists, else Insert the document.
* Delete - Delete one or more documents from the collection, depending on what is supplied to "Filter"

# Maven Build target cheatsheet :
mvn clean compile assembly:single
