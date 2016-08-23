/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;

/**
 * @author Davide D'Alto
 */
public final class RemoteNeo4jDialectOperations {

	private RemoteNeo4jDialectOperations() {
	}

	public static Map<String, Node> findToOneEntities(Transaction tx, NodeWithEmbeddedNodes owner, EntityKey entityKey, TupleContext tupleContext, RemoteNeo4jEntityQueries queries) {
		return findToOneEntities( tx, owner, entityKey.getMetadata(), tupleContext, queries );
	}

	public static Map<String, Node> findToOneEntities(Transaction tx, NodeWithEmbeddedNodes node, EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext, RemoteNeo4jEntityQueries queries) {
		Map<String, Node> toOneEntities = new HashMap<>( tupleContext.getAllAssociatedEntityKeyMetadata().size() );
		if ( tupleContext.getAllAssociatedEntityKeyMetadata().size() > 0 ) {
			Object[] keyValues = keyValues( node.getOwner(), entityKeyMetadata );
			for ( Entry<String, AssociatedEntityKeyMetadata> entry : tupleContext.getAllAssociatedEntityKeyMetadata().entrySet() ) {
				String associationRole = tupleContext.getAllRoles().get( entry.getKey() );
				Node associatedEntity = queries.findAssociatedEntity( tx, keyValues, associationRole );
				toOneEntities.put( associationRole, associatedEntity );
			}
		}
		return toOneEntities;
	}

	private static Object[] keyValues(Node node, EntityKeyMetadata entityKeyMetadata) {
		Object[] values = new Object[entityKeyMetadata.getColumnNames().length];
		for ( int i = 0; i < values.length; i++ ) {
			values[i] = node.get( entityKeyMetadata.getColumnNames()[i] );
		}
		return values;
	}
}
