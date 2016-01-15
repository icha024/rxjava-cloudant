package com.clianz.cloudant.rx;

import com.cloudant.client.api.Changes;
import com.cloudant.client.api.model.ChangesResult;
import rx.Observable;
import rx.Subscriber;

public class CloudantChangesRx {

	private Changes changes;

	public CloudantChangesRx(Changes changes) {
		this.changes = changes;
	}


	/**
	 * RX methods
	 */

	/**
	 * These are not required. Everything is now encapsulated in 'getChanges()' rx stream.
	 *
	 * <p>
	 * public boolean hasNext() {
	 * return changes.hasNext();
	 * }
	 * <p>
	 * public ChangesResult.Row next() {
	 * return changes.next();
	 * }
	 * <p>
	 * public ChangesResult getChanges() {
	 * return changes.getChanges();
	 * }
	 */

	public Observable<ChangesResult.Row> getChanges() {
		return Observable.create(new Observable.OnSubscribe<ChangesResult.Row>() {
			@Override
			public void call(Subscriber<? super ChangesResult.Row> subscriber) {
				try {
					while (changes.hasNext()) {
						subscriber.onNext(changes.next());
					}
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> stop() {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					changes.stop();
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

	public CloudantChangesRx continuousChanges() {
		changes.continuousChanges();
		return this;
	}

	public CloudantChangesRx limit(int limit) {
		changes.limit(limit);
		return this;
	}

	public CloudantChangesRx includeDocs(boolean includeDocs) {
		changes.includeDocs(includeDocs);
		return this;
	}

	public CloudantChangesRx since(String since) {
		changes.since(since);
		return this;
	}

	public CloudantChangesRx style(String style) {
		changes.style(style);
		return this;
	}

	public CloudantChangesRx heartBeat(long heartBeat) {
		changes.heartBeat(heartBeat);
		return this;
	}

	public CloudantChangesRx filter(String filter) {
		changes.filter(filter);
		return this;
	}

	public CloudantChangesRx timeout(long timeout) {
		changes.timeout(timeout);
		return this;
	}
}
