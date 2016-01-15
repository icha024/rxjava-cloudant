package com.clianz.cloudant.rx;

import com.cloudant.client.api.Replication;
import com.cloudant.client.api.model.ReplicationResult;
import rx.Observable;
import rx.Subscriber;

import java.util.Map;

public class CloudantReplicationRx {

	private Replication replication;

	public CloudantReplicationRx(Replication replication) {
		this.replication = replication;
	}

	/**
	 * Get the underlying non-rx Cloudant replication.
	 *
	 * @return Cloudant replication
	 */
	public Replication getNonRxReplication() {
		return replication;
	}

	/**
	 * RX method
	 */

	public Observable<ReplicationResult> trigger() {
		return Observable.create(new Observable.OnSubscribe<ReplicationResult>() {
			@Override
			public void call(Subscriber<? super ReplicationResult> subscriber) {
				try {
					subscriber.onNext(replication.trigger());
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

	public CloudantReplicationRx cancel(Boolean cancel) {
		replication.cancel(cancel);
		return this;
	}

	public CloudantReplicationRx targetOauth(String consumerSecret, String consumerKey, String tokenSecret, String token) {
		replication.targetOauth(consumerSecret, consumerKey, tokenSecret, token);
		return this;
	}

	public CloudantReplicationRx source(String source) {
		replication.source(source);
		return this;
	}

	public CloudantReplicationRx queryParams(String queryParams) {
		replication.queryParams(queryParams);
		return this;
	}

	public CloudantReplicationRx createTarget(Boolean createTarget) {
		replication.createTarget(createTarget);
		return this;
	}

	public CloudantReplicationRx target(String target) {
		replication.target(target);
		return this;
	}

	public CloudantReplicationRx docIds(String... docIds) {
		replication.docIds(docIds);
		return this;
	}

	public CloudantReplicationRx filter(String filter) {
		replication.filter(filter);
		return this;
	}

	public CloudantReplicationRx continuous(Boolean continuous) {
		replication.continuous(continuous);
		return this;
	}

	public CloudantReplicationRx queryParams(Map<String, Object> queryParams) {
		replication.queryParams(queryParams);
		return this;
	}

	public CloudantReplicationRx proxy(String proxy) {
		replication.proxy(proxy);
		return this;
	}

	public CloudantReplicationRx sinceSeq(Integer sinceSeq) {
		replication.sinceSeq(sinceSeq);
		return this;
	}
}
