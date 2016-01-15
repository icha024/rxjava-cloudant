package com.clianz.cloudant.rx;

import com.clianz.bluemix.configurator.BluemixConfigStore;
import com.clianz.cloudant.rx.internal.CredentialUtils;
import com.cloudant.client.api.*;
import com.cloudant.client.api.model.*;
import com.cloudant.client.api.views.AllDocsRequestBuilder;
import com.cloudant.client.api.views.ViewRequestBuilder;
import rx.Observable;
import rx.Subscriber;

import java.io.InputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class CloudantDatabaseRx {

	private Database db = null;

	public CloudantDatabaseRx(String dbName) {
		this(CredentialUtils.getCloudantUsername(), CredentialUtils.getCloudantPassword(), dbName);
	}

	public CloudantDatabaseRx(String accountName, String username, String password, String dbName) {
		this(accountName, username, password, dbName, true);
	}

	public CloudantDatabaseRx(String username, String password, String dbName) {
		this(username, username, password, dbName, true);
	}

	public CloudantDatabaseRx(String accountName, String username, String password, String dbName, boolean createDb) {
		CloudantClient client = ClientBuilder.account(accountName).username(username).password(password).build();
		this.db = client.database(dbName, createDb);
	}

	public CloudantDatabaseRx(CloudantClient client, String dbName, boolean createDb) {
		this.db = client.database(dbName, createDb);
	}

	public CloudantDatabaseRx(CloudantClient client, String dbName) {
		this(client, dbName, true);
	}

	public CloudantDatabaseRx(Database db) {
		this.db = db;
	}

	/**
	 * Get the underlying non-rx Cloudant database.
	 *
	 * @ return Cloudant database
	 */
	public Database getNonRxDatabase() {
		return db;
	}

	public CloudantDesignDocumentManagerRx getDesignDocumentManager() {
		return new CloudantDesignDocumentManagerRx(db.getDesignDocumentManager());
	}

	/**
	 * Rx Methods
	 */

	public Observable<InputStream> find(final String id) {
		return Observable.create(new Observable.OnSubscribe<InputStream>() {
			@Override
			public void call(Subscriber<? super InputStream> subscriber) {
				try {
					InputStream inputStream = db.find(id);
					subscriber.onNext(inputStream);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> save(final Object object) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					Response response = db.save(object);
					subscriber.onNext(response);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> update(final Object object) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					Response response = db.update(object);
					subscriber.onNext(response);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> remove(final Object object) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					Response response = db.remove(object);
					subscriber.onNext(response);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<Response>> bulk(final List<?> objects) {
		return Observable.create(new Observable.OnSubscribe<List<Response>>() {
			@Override
			public void call(Subscriber<? super List<Response>> subscriber) {
				try {
					List<Response> response = db.bulk(objects);
					subscriber.onNext(response);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> setPermissions(final String userNameorApikey, final EnumSet<Permissions> permissions) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					db.setPermissions(userNameorApikey, permissions);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> deleteIndex(final String indexName, final String designDocId) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					db.deleteIndex(indexName, designDocId);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<List<T>> findByIndex(final String selectorJson, final Class<T> classOfT, final FindByIndexOptions options) {
		return Observable.create(new Observable.OnSubscribe<List<T>>() {
			@Override
			public void call(Subscriber<? super List<T>> subscriber) {
				try {
					subscriber.onNext(db.findByIndex(selectorJson, classOfT, options));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> post(final Object object) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.post(object));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<URI> getDBUri() {
		return Observable.create(new Observable.OnSubscribe<URI>() {
			@Override
			public void call(Subscriber<? super URI> subscriber) {
				try {
					subscriber.onNext(db.getDBUri());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Shard> getShard(final String docId) {
		return Observable.create(new Observable.OnSubscribe<Shard>() {
			@Override
			public void call(Subscriber<? super Shard> subscriber) {
				try {
					subscriber.onNext(db.getShard(docId));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<InputStream> find(final String id, final String rev) {
		return Observable.create(new Observable.OnSubscribe<InputStream>() {
			@Override
			public void call(Subscriber<? super InputStream> subscriber) {
				try {
					subscriber.onNext(db.find(id, rev));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<CloudantChangesRx> changes() {
		return Observable.create(new Observable.OnSubscribe<CloudantChangesRx>() {
			@Override
			public void call(Subscriber<? super CloudantChangesRx> subscriber) {
				try {
					subscriber.onNext(new CloudantChangesRx(db.changes()));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> saveAttachment(final InputStream in, final String name, final String contentType, final String docId, final String docRev) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.saveAttachment(in, name, contentType, docId, docRev));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> save(final Object object, final int writeQuorum) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.save(object, writeQuorum));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> createIndex(final String indexName, final String designDocName, final String indexType, final IndexField[] fields) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					db.createIndex(indexName, designDocName, indexType, fields);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<T> findAny(final Class<T> classType, final String uri) {
		return Observable.create(new Observable.OnSubscribe<T>() {
			@Override
			public void call(Subscriber<? super T> subscriber) {
				try {
					subscriber.onNext(db.findAny(classType, uri));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> remove(final String id, final String rev) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.remove(id, rev));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Boolean> contains(final String id) {
		return Observable.create(new Observable.OnSubscribe<Boolean>() {
			@Override
			public void call(Subscriber<? super Boolean> subscriber) {
				try {
					subscriber.onNext(db.contains(id));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> update(final Object object, final int writeQuorum) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.update(object, writeQuorum));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<Shard>> getShards() {
		return Observable.create(new Observable.OnSubscribe<List<Shard>>() {
			@Override
			public void call(Subscriber<? super List<Shard>> subscriber) {
				try {
					subscriber.onNext(db.getShards());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<T> find(final Class<T> classType, final String id) {
		return Observable.create(new Observable.OnSubscribe<T>() {
			@Override
			public void call(Subscriber<? super T> subscriber) {
				try {
					subscriber.onNext(db.find(classType, id));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<String> invokeUpdateHandler(final String updateHandlerUri, final String docId, final Params params) {
		return Observable.create(new Observable.OnSubscribe<String>() {
			@Override
			public void call(Subscriber<? super String> subscriber) {
				try {
					subscriber.onNext(db.invokeUpdateHandler(updateHandlerUri, docId, params));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<T> find(final Class<T> classType, final String id, final String rev) {
		return Observable.create(new Observable.OnSubscribe<T>() {
			@Override
			public void call(Subscriber<? super T> subscriber) {
				try {
					subscriber.onNext(db.find(classType, id, rev));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Map<String, EnumSet<Permissions>>> getPermissions() {
		return Observable.create(new Observable.OnSubscribe<Map<String, EnumSet<Permissions>>>() {
			@Override
			public void call(Subscriber<? super Map<String, EnumSet<Permissions>>> subscriber) {
				try {
					subscriber.onNext(db.getPermissions());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<Index>> listIndices() {
		return Observable.create(new Observable.OnSubscribe<List<Index>>() {
			@Override
			public void call(Subscriber<? super List<Index>> subscriber) {
				try {
					subscriber.onNext(db.listIndices());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> post(final Object object, final int writeQuorum) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.post(object, writeQuorum));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> ensureFullCommit() {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					db.ensureFullCommit();
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<List<T>> findByIndex(final String selectorJson, final Class<T> classOfT) {
		return Observable.create(new Observable.OnSubscribe<List<T>>() {
			@Override
			public void call(Subscriber<? super List<T>> subscriber) {
				try {
					subscriber.onNext(db.findByIndex(selectorJson, classOfT));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<CloudantSearchRx> search(final String searchIndexId) {
		return Observable.create(new Observable.OnSubscribe<CloudantSearchRx>() {
			@Override
			public void call(Subscriber<? super CloudantSearchRx> subscriber) {
				try {
					subscriber.onNext(new CloudantSearchRx(db.search(searchIndexId)));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> createIndex(final String indexDefinition) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					db.createIndex(indexDefinition);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<DbInfo> info() {
		return Observable.create(new Observable.OnSubscribe<DbInfo>() {
			@Override
			public void call(Subscriber<? super DbInfo> subscriber) {
				try {
					subscriber.onNext(db.info());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<T> find(final Class<T> classType, final String id, final Params params) {
		return Observable.create(new Observable.OnSubscribe<T>() {
			@Override
			public void call(Subscriber<? super T> subscriber) {
				try {
					subscriber.onNext(db.find(classType, id, params));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> saveAttachment(final InputStream in, final String name, final String contentType) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(db.saveAttachment(in, name, contentType));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<ViewRequestBuilder> getViewRequestBuilder(final String designDoc, final String viewName) {
		return Observable.create(new Observable.OnSubscribe<ViewRequestBuilder>() {
			@Override
			public void call(Subscriber<? super ViewRequestBuilder> subscriber) {
				try {
					subscriber.onNext(db.getViewRequestBuilder(designDoc, viewName));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<AllDocsRequestBuilder> getAllDocsRequestBuilder() {
		return Observable.create(new Observable.OnSubscribe<AllDocsRequestBuilder>() {
			@Override
			public void call(Subscriber<? super AllDocsRequestBuilder> subscriber) {
				try {
					subscriber.onNext(db.getAllDocsRequestBuilder());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}
}
