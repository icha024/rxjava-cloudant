package com.clianz.cloudant.rx;

import com.cloudant.client.api.Search;
import com.cloudant.client.api.model.SearchResult;
import rx.Observable;
import rx.Subscriber;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CloudantSearchRx {

	private Search search;

	public CloudantSearchRx(Search search) {
		this.search = search;
	}

	/**
	 * Get the underlying non-rx Cloudant search.
	 *
	 * @return Cloudant search
	 */
	public Search getNonRxSearch() {
		return search;
	}

	/**
	 * RX method
	 */

	public Observable<InputStream> queryForStream(final String query) {
		return Observable.create(new Observable.OnSubscribe<InputStream>() {
			@Override
			public void call(Subscriber<? super InputStream> subscriber) {
				try {
					subscriber.onNext(search.queryForStream(query));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<Map<String, List<T>>> queryGroups(final String query, final Class<T> classOfT) {
		return Observable.create(new Observable.OnSubscribe<Map<String, List<T>>>() {
			@Override
			public void call(Subscriber<? super Map<String, List<T>>> subscriber) {
				try {
					subscriber.onNext(search.queryGroups(query, classOfT));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<SearchResult<T>> querySearchResult(final String query, final Class<T> classOfT) {
		return Observable.create(new Observable.OnSubscribe<SearchResult<T>>() {
			@Override
			public void call(Subscriber<? super SearchResult<T>> subscriber) {
				try {
					subscriber.onNext(search.querySearchResult(query, classOfT));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	public <T> Observable<List<T>> query(final String query, final Class<T> classOfT) {
		return Observable.create(new Observable.OnSubscribe<List<T>>() {
			@Override
			public void call(Subscriber<? super List<T>> subscriber) {
				try {
					subscriber.onNext(search.query(query, classOfT));
				} catch (Exception e) {
					subscriber.onError(e.getCause());
				} finally {
					subscriber.onCompleted();
				}
			}
		});
	}

	/**
	 * Delegate method
	 */

	public CloudantSearchRx stale(boolean stale) {
		search.stale(stale);
		return this;
	}

	public CloudantSearchRx includeDocs(Boolean includeDocs) {
		search.includeDocs(includeDocs);
		return this;
	}

	public CloudantSearchRx sort(String sortJson) {
		search.sort(sortJson);
		return this;
	}

	public CloudantSearchRx drillDown(String fieldName, String fieldValue) {
		search.drillDown(fieldName, fieldValue);
		return this;
	}

	public CloudantSearchRx groupLimit(int limit) {
		search.groupLimit(limit);
		return this;
	}

	public CloudantSearchRx groupField(String fieldName, boolean isNumber) {
		search.groupField(fieldName, isNumber);
		return this;
	}

	public CloudantSearchRx limit(Integer limit) {
		search.limit(limit);
		return this;
	}

	public CloudantSearchRx ranges(String rangesJson) {
		search.ranges(rangesJson);
		return this;
	}

	public CloudantSearchRx groupSort(String groupsortJson) {
		search.groupSort(groupsortJson);
		return this;
	}

	public CloudantSearchRx bookmark(String bookmark) {
		search.bookmark(bookmark);
		return this;
	}

	public CloudantSearchRx counts(String[] countsfields) {
		search.counts(countsfields);
		return this;
	}
}
