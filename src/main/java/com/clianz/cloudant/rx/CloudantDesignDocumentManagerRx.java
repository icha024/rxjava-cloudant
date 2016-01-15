package com.clianz.cloudant.rx;

import com.cloudant.client.api.DesignDocumentManager;
import com.cloudant.client.api.model.DesignDocument;
import com.cloudant.client.api.model.Response;
import rx.Observable;
import rx.Subscriber;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public class CloudantDesignDocumentManagerRx {

	private DesignDocumentManager designDocumentManager;

	public CloudantDesignDocumentManagerRx(DesignDocumentManager designDocumentManager) {
		this.designDocumentManager = designDocumentManager;
	}

	/**
	 * Get the underlying non-rx Cloudant design document manager.
	 *
	 * @return Cloudant design document manager
	 */
	public DesignDocumentManager getNonRxDesignDocumentManager() {
		return designDocumentManager;
	}

	/**
	 * RX methods
	 */

	public Observable<Response> put(final DesignDocument document) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(designDocumentManager.put(document));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> remove(final DesignDocument designDocument) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(designDocumentManager.remove(designDocument));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<DesignDocument> get(final String id, final String rev) {
		return Observable.create(new Observable.OnSubscribe<DesignDocument>() {
			@Override
			public void call(Subscriber<? super DesignDocument> subscriber) {
				try {
					subscriber.onNext(designDocumentManager.get(id, rev));
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
					subscriber.onNext(designDocumentManager.remove(id, rev));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<DesignDocument> get(final String id) {
		return Observable.create(new Observable.OnSubscribe<DesignDocument>() {
			@Override
			public void call(Subscriber<? super DesignDocument> subscriber) {
				try {
					subscriber.onNext(designDocumentManager.get(id));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public static Observable<DesignDocument> fromFile(final File file) throws FileNotFoundException {
		return Observable.create(new Observable.OnSubscribe<DesignDocument>() {
			@Override
			public void call(Subscriber<? super DesignDocument> subscriber) {
				try {
					subscriber.onNext(DesignDocumentManager.fromFile(file));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Response> remove(final String id) {
		return Observable.create(new Observable.OnSubscribe<Response>() {
			@Override
			public void call(Subscriber<? super Response> subscriber) {
				try {
					subscriber.onNext(designDocumentManager.remove(id));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public static Observable<List<DesignDocument>> fromDirectory(final File directory) throws FileNotFoundException {
		return Observable.create(new Observable.OnSubscribe<List<DesignDocument>>() {
			@Override
			public void call(Subscriber<? super List<DesignDocument>> subscriber) {
				try {
					subscriber.onNext(DesignDocumentManager.fromDirectory(directory));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public Observable<Void> put(final DesignDocument... designDocs) {
		return Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> subscriber) {
				try {
					designDocumentManager.put(designDocs);
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}
}
