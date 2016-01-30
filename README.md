# RxJava-Cloudant

Reactive extension wrapper ([RxJava](https://github.com/ReactiveX/RxJava)) for Cloudant client library.

## Usage
Wrap the Rx type around it's Non-Rx equivalent, using the constructor provided, to create an RX type.

Example for creating Rx Cloudant client: 
```java
CloudantClientRx rxClient = new CloudantClientRx(normalCloudantClient)
```

Example creating a Rx Cloudant database:
```java
CloudantDatabaseRx rxDatabase = new CloudantDatabaseRx(normalCloudantDatabase)
```

## Available Rx wrapper types:
- CloudantChangeRx
- CloudantClientRx
- CloudantDatabaseRx
- CloudantDesignDocumentManagerRx
- CloudantReplicationRx
- CloudantReplicatorRx
- CloudantSearchRx

## Requirements
- Java 1.6+
- Official Cloudant client 2.x
