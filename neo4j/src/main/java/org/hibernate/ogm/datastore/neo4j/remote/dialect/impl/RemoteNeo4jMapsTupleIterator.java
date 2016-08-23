/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.ogm.datastore.map.impl.MapTupleSnapshot;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.model.spi.Tuple;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

/**
 * Iterates over the results of a native query when each result is not mapped by an entity
 *
 * @author Davide D'Alto
 */
public class RemoteNeo4jMapsTupleIterator implements ClosableIterator<Tuple> {

	private final StatementResult statementResult;
	private final List<String> columns;

	public RemoteNeo4jMapsTupleIterator(StatementResult statementResult) {
		this.statementResult = statementResult;
		this.columns = statementResult.keys();
	}

	@Override
	public boolean hasNext() {
		return statementResult.hasNext();
	}

	@Override
	public void remove() {
		statementResult.remove();
	}

	@Override
	public Tuple next() {
		return convert( statementResult.next() );
	}

	@Override
	public void close() {
	}

	protected Tuple convert(Record record) {
		// Requires a LinkedHashMap as the order of the entries is important
		Map<String, Object> properties = new LinkedHashMap<>();
		for ( String column : columns ) {
			Value value = record.get( column );
			if ( value != null && !value.isNull() ) {
				properties.put( column, value.asObject() );
			}
			else {
				properties.put( column, null );
			}
		}
		return new Tuple( new MapTupleSnapshot( properties ) );
	}
}
