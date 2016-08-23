/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.util.impl;

import org.hibernate.ogm.model.key.spi.RowKey;
import org.neo4j.driver.v1.types.Node;

/**
 * @author Davide D'Alto
 */
public class RemoteNeo4jHelper {

	private RemoteNeo4jHelper() {
	}

	/**
	* Check if the node matches the column values
	*
	* @param node the {@link Node} to verify
	* @param columnNames the expected name of the key columns
	* @param columnValues the expected value of the key columns
	* @return true if the node matches the key columns and values
	*/
	public static boolean matches(Node node, String[] columnNames, Object[] columnValues) {
		for ( int i = 0; i < columnNames.length; i++ ) {
			String property = columnNames[i];
			Object expectedValue = columnValues[i];
			boolean containsProperty = node.containsKey( property );
			if ( containsProperty ) {
				Object actualValue = node.get( property ).asObject();
				if ( !sameValue( expectedValue, actualValue ) ) {
					return false;
				}
			}
			else if ( expectedValue != null ) {
				return false;
			}
		}
		return true;
	}

	public static boolean matches(RowKey actual, RowKey expected) {
		if ( actual.getColumnNames().length != expected.getColumnNames().length) {
			return false;
		}
		for ( int i = 0; i < expected.getColumnNames().length; i++ ) {
			Object expectedValue = expected.getColumnValues()[i];
			Object actualValue = actual.getColumnValues()[i];
			if ( !sameValue( expectedValue, actualValue ) ) {
				return false;
			}
		}
		return true;
	}

	private static boolean sameValue(Object expectedValue, Object actualValue) {
		if ( actualValue == null && expectedValue != null ) {
			return false;
		}
		if ( actualValue != null && !actualValue.equals( expectedValue ) ) {
			// Neo4j remote does not save the type of the original value, for example if the original value was
			// a Long smaller than the max integer the query will return the value as integer.
			if ( !actualValue.toString().equals( expectedValue.toString() ) ) {
				return false;
			}
		}
		return true;
	}
}
