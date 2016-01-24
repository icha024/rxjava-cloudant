package com.clianz.cloudant.rx;

import com.clianz.cloudant.rx.internal.CredentialUtils;
import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.model.ApiKey;
import com.cloudant.client.api.model.Membership;
import com.cloudant.client.api.model.Task;
import com.cloudant.http.HttpConnection;
import com.google.gson.Gson;
import rx.Observable;
import rx.Subscriber;

import java.net.URI;
import java.util.List;

public class CloudantClientRx {

	private CloudantClient client;

	public CloudantClientRx() {
		String username = CredentialUtils.getCloudantUsername();
		String password =  CredentialUtils.getCloudantPassword();
		client = ClientBuilder.account(username).username(username).password(password).build();
	}

	public CloudantClientRx(String accountName, String username, String password) {
		client = ClientBuilder.account(accountName).username(username).password(password).build();
	}

	public CloudantDatabaseRx database(String dbName, boolean create) {
		return new CloudantDatabaseRx(client.database(dbName, create));
	}

	public CloudantClientRx(CloudantClient client) {
		this.client = client;
	}

	/**
	 * Get the underlying non-rx Cloudant client.
	 * @return Cloudant client
	 */
	public CloudantClient getNonRxClient() {
		return client;
	}

	/** RX method */

	public Observable<ApiKey> generateApiKey() {
		return Observable.create(new Observable.OnSubscribe<ApiKey>() {
			@Override
			public void call(Subscriber<? super ApiKey> subscriber) {
				try {
					subscriber.onNext(client.generateApiKey());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<Task>> getActiveTasks() {
		return Observable.create(new Observable.OnSubscribe<List<Task>>() {
			@Override
			public void call(Subscriber<? super List<Task>> subscriber) {
				try {
					subscriber.onNext(client.getActiveTasks());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Membership> getMembership() {
		return Observable.create(new Observable.OnSubscribe<Membership>() {
			@Override
			public void call(Subscriber<? super Membership> subscriber) {
				try {
					subscriber.onNext(client.getMembership());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<HttpConnection> executeRequest(final HttpConnection request) {
		return Observable.create(new Observable.OnSubscribe<HttpConnection>() {
			@Override
			public void call(Subscriber<? super HttpConnection> subscriber) {
				try {
					subscriber.onNext(client.executeRequest(request));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<String>> getAllDbs() {
		return Observable.create(new Observable.OnSubscribe<List<String>>() {
			@Override
			public void call(Subscriber<? super List<String>> subscriber) {
				try {
					subscriber.onNext(client.getAllDbs());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Gson> getGson() {
		return Observable.create(new Observable.OnSubscribe<Gson>() {
			@Override
			public void call(Subscriber<? super Gson> subscriber) {
				try {
					subscriber.onNext(client.getGson());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<CloudantReplicatorRx> replicator() {
		return Observable.create(new Observable.OnSubscribe<CloudantReplicatorRx>() {
			@Override
			public void call(Subscriber<? super CloudantReplicatorRx> subscriber) {
				try {
					subscriber.onNext(new CloudantReplicatorRx(client.replicator()));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<URI> getBaseUri() {
		return Observable.create(new Observable.OnSubscribe<URI>() {
			@Override
			public void call(Subscriber<? super URI> subscriber) {
				try {
					subscriber.onNext(client.getBaseUri());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> createDB(final String dbName) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					client.createDB(dbName);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<CloudantReplicationRx> replication() {
		return Observable.create(new Observable.OnSubscribe<CloudantReplicationRx>() {
			@Override
			public void call(Subscriber<? super CloudantReplicationRx> subscriber) {
				try {
					subscriber.onNext(new CloudantReplicationRx(client.replication()));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<String> serverVersion() {
		return Observable.create(new Observable.OnSubscribe<String>() {
			@Override
			public void call(Subscriber<? super String> subscriber) {
				try {
					subscriber.onNext(client.serverVersion());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<String>> uuids(final long count) {
		return Observable.create(new Observable.OnSubscribe<List<String>>() {
			@Override
			public void call(Subscriber<? super List<String>> subscriber) {
				try {
					subscriber.onNext(client.uuids(count));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> deleteDB(final String dbName) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					client.deleteDB(dbName);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> shutdown() {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					client.shutdown();
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}
}
