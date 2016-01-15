package com.clianz.cloudant.rx;

import com.cloudant.client.api.Replicator;
import com.cloudant.client.api.model.ReplicatorDocument;
import com.cloudant.client.api.model.Response;
import rx.Observable;
import rx.Subscriber;

import java.util.List;
import java.util.Map;

public class CloudantReplicatorRx {

	private Replicator replicator;

	public CloudantReplicatorRx(Replicator replicator) {
		this.replicator = replicator;
	}

	/**
	 * Get the underlying non-rx Cloudant replicator.
	 *
	 * @return Cloudant replicator
	 */
	public Replicator getNonRxReplicator() {
		return replicator;
	}

	/**
	 * RX method
	 */

	public Observable<Response> save() {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(replicator.save());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<ReplicatorDocument> find() {
		return Observable.create(new Observable.OnSubscribe<ReplicatorDocument>() {
			@Override
			public void call(Subscriber<? super ReplicatorDocument> subscriber) {
				try {
					subscriber.onNext(replicator.find());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> remove() {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(replicator.remove());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<List<ReplicatorDocument>> findAll() {
		return Observable.create(new Observable.OnSubscribe<List<ReplicatorDocument>>() {
			@Override
			public void call(Subscriber<? super List<ReplicatorDocument>> subscriber) {
				try {
					subscriber.onNext(replicator.findAll());
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	/**
	 * Delegate methods
	 */

	public CloudantReplicatorRx queryParams(String queryParams) {
		replicator.queryParams(queryParams);
		return this;
	}

	public CloudantReplicatorRx userCtxRoles(String... userCtxRoles) {
		replicator.userCtxRoles(userCtxRoles);
		return this;
	}

	public CloudantReplicatorRx sinceSeq(Integer sinceSeq) {
		replicator.sinceSeq(sinceSeq);
		return this;
	}

	public CloudantReplicatorRx workerProcesses(int workerProcesses) {
		replicator.workerProcesses(workerProcesses);
		return this;
	}

	public CloudantReplicatorRx replicatorDocId(String replicatorDocId) {
		replicator.replicatorDocId(replicatorDocId);
		return this;
	}

	public CloudantReplicatorRx retriesPerRequest(int retriesPerRequest) {
		replicator.retriesPerRequest(retriesPerRequest);
		return this;
	}

	public CloudantReplicatorRx continuous(boolean continuous) {
		replicator.continuous(continuous);
		return this;
	}

	public CloudantReplicatorRx createTarget(Boolean createTarget) {
		replicator.createTarget(createTarget);
		return this;
	}

	public CloudantReplicatorRx target(String target) {
		replicator.target(target);
		return this;
	}

	public CloudantReplicatorRx filter(String filter) {
		replicator.filter(filter);
		return this;
	}

	public CloudantReplicatorRx connectionTimeout(long connectionTimeout) {
		replicator.connectionTimeout(connectionTimeout);
		return this;
	}

	public CloudantReplicatorRx proxy(String proxy) {
		replicator.proxy(proxy);
		return this;
	}

	public CloudantReplicatorRx httpConnections(int httpConnections) {
		replicator.httpConnections(httpConnections);
		return this;
	}

	public CloudantReplicatorRx docIds(String... docIds) {
		replicator.docIds(docIds);
		return this;
	}

	public CloudantReplicatorRx replicatorDB(String replicatorDB) {
		replicator.replicatorDB(replicatorDB);
		return this;
	}

	public CloudantReplicatorRx source(String source) {
		replicator.source(source);
		return this;
	}

	public CloudantReplicatorRx queryParams(Map<String, Object> queryParams) {
		replicator.queryParams(queryParams);
		return this;
	}

	public CloudantReplicatorRx userCtxName(String userCtxName) {
		replicator.userCtxName(userCtxName);
		return this;
	}

	public CloudantReplicatorRx replicatorDocRev(String replicatorDocRev) {
		replicator.replicatorDocRev(replicatorDocRev);
		return this;
	}

	public CloudantReplicatorRx workerBatchSize(int workerBatchSize) {
		replicator.workerBatchSize(workerBatchSize);
		return this;
	}
}
