/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.dialect.impl;

import static org.hibernate.ogm.datastore.neo4j.dialect.impl.NodeLabel.EMBEDDED;
import static org.hibernate.ogm.datastore.neo4j.dialect.impl.NodeLabel.ENTITY;
import static org.hibernate.ogm.datastore.neo4j.query.parsing.cypherdsl.impl.CypherDSL.escapeIdentifier;
import static org.hibernate.ogm.util.impl.EmbeddedHelper.isPartOfEmbedded;
import static org.hibernate.ogm.util.impl.EmbeddedHelper.split;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hibernate.internal.util.collections.BoundedConcurrentHashMap;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.util.impl.EmbeddedHelper;

/**
 * @author Davide D'Alto
 */
public class EntityQueries extends QueriesBase {

	private static final int CACHE_CAPACITY = 1000;
	private static final int CACHE_CONCURRENCY_LEVEL = 20;

	private final Map<String, String> findEmbeddedNodeQueries;
	private final Map<String, String> removeEmbeddedPropertyQuery;
	private final Map<String, String> removePropertyQueries;

	private final BoundedConcurrentHashMap<String, String> updateEmbeddedPropertyQueryCache;
	private final BoundedConcurrentHashMap<String, String> findAssociationQueryCache;

	private final String removeToOneAssociation;
	private final String createEmbeddedNodeQuery;
	private final String findEntityQuery;
	private final String findEntitiesQuery;
	private final String findAssociationPartialQuery;
	private final String createEntityQuery;
	private final String createEntityWithPropertiesQuery;
	private final String updateEntityProperties;
	private final String removeEntityQuery;
	private final String updateEmbeddedNodeQuery;
	private final Map<String, String> updateToOneQuery;
	private final Map<String, String> findAssociatedEntityQuery;

	private final EntityKeyMetadata entityKeyMetadata;

	public EntityQueries(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		this.entityKeyMetadata = entityKeyMetadata;
		this.updateEmbeddedPropertyQueryCache = new BoundedConcurrentHashMap<String, String>( CACHE_CAPACITY, CACHE_CONCURRENCY_LEVEL, BoundedConcurrentHashMap.Eviction.LIRS );
		this.findAssociationQueryCache = new BoundedConcurrentHashMap<String, String>( CACHE_CAPACITY, CACHE_CONCURRENCY_LEVEL, BoundedConcurrentHashMap.Eviction.LIRS );

		this.findAssociationPartialQuery = initMatchOwnerEntityNode( entityKeyMetadata );
		this.createEmbeddedNodeQuery = initCreateEmbeddedNodeQuery( entityKeyMetadata );
		this.findEntityQuery = initFindEntityQuery( entityKeyMetadata );
		this.findEntitiesQuery = initFindEntitiesQuery( entityKeyMetadata );
		this.createEntityQuery = initCreateEntityQuery( entityKeyMetadata );
		this.updateEntityProperties = initMatchOwnerEntityNode( entityKeyMetadata );
		this.createEntityWithPropertiesQuery = initCreateEntityWithPropertiesQuery( entityKeyMetadata );
		this.removeEntityQuery = initRemoveEntityQuery( entityKeyMetadata );
		this.updateEmbeddedNodeQuery = initUpdateEmbeddedNodeQuery( entityKeyMetadata );
		this.updateToOneQuery = initUpdateToOneQuery( entityKeyMetadata, tupleContext );
		this.findAssociatedEntityQuery = initFindAssociatedEntityQuery( entityKeyMetadata, tupleContext );
		this.findEmbeddedNodeQueries = initFindEmbeddedNodeQuery( entityKeyMetadata, tupleContext );
		this.removeEmbeddedPropertyQuery = initRemoveEmbeddedPropertyQuery( entityKeyMetadata, tupleContext );
		this.removePropertyQueries = initRemovePropertyQueries( entityKeyMetadata, tupleContext );
		this.removeToOneAssociation = initRemoveToOneAssociation( entityKeyMetadata, tupleContext );
	}

	private String initRemoveToOneAssociation(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		StringBuilder queryBuilder = new StringBuilder();
		appendMatchOwnerEntityNode( queryBuilder, entityKeyMetadata );
		queryBuilder.append( " -[r]-> (:" );
		queryBuilder.append( NodeLabel.ENTITY );
		queryBuilder.append( ") WHERE type(r) = {");
		queryBuilder.append( entityKeyMetadata.getColumnNames().length );
		queryBuilder.append( "} DELETE r" );
		return queryBuilder.toString();
	}

	private Map<String, String> initRemovePropertyQueries(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		if ( tupleContext == null ) {
			return Collections.emptyMap();
		}

		Map<String, String> removeColumn = new HashMap<>();
		for ( String column : tupleContext.getSelectableColumns() ) {
			if ( !column.contains( "." ) ) {
				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append( "MATCH " );
				appendEntityNode( "n", entityKeyMetadata, queryBuilder );
				queryBuilder.append( " REMOVE n." );
				escapeIdentifier( queryBuilder, column );

				removeColumn.put( column, queryBuilder.toString() );
			}
		}
		return Collections.unmodifiableMap( removeColumn );
	}

	/*
	 * MATCH (owner:ENTITY:Account {login: {0}}) -[:type]-> (e:EMBEDDED)
	 * REMOVE e.property
	 */
	private Map<String, String> initRemoveEmbeddedPropertyQuery(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		if ( tupleContext == null ) {
			return Collections.emptyMap();
		}
		Map<String, String> removeColumn = new HashMap<>();
		for ( String column : tupleContext.getSelectableColumns() ) {
			if ( EmbeddedHelper.isPartOfEmbedded( column ) ) {
				if ( !removeColumn.containsKey( column ) ) {
					StringBuilder queryBuilder = new StringBuilder();
					queryBuilder.append( "MATCH " );
					appendEntityNode( "n", entityKeyMetadata, queryBuilder );
					String[] path = EmbeddedHelper.split( column );
					for ( int i = 0; i < path.length - 1; i++ ) {
						queryBuilder.append( "-[:" );
						appendRelationshipType( queryBuilder, path[i] );
						queryBuilder.append( "]->" );
						if ( i == path.length - 2 ) {
							queryBuilder.append( "(e:EMBEDDED) " );
						}
						else {
							queryBuilder.append( "(:EMBEDDED) " );
						}
					}
					queryBuilder.append( "REMOVE e." );
					escapeIdentifier( queryBuilder, path[path.length - 1] );
					queryBuilder.append( " WITH e ");
					queryBuilder.append( "MATCH (e)<-[erel]-(a) ");
					queryBuilder.append( "WHERE length(keys(e))=0 AND NOT ((e)-->()) ");
					queryBuilder.append( "DELETE e, erel ");
					queryBuilder.append( "WITH a ");
					queryBuilder.append( "OPTIONAL MATCH path=(a)<-[r*]-(b:EMBEDDED), (b)<-[brel]-(), (x) ");
					queryBuilder.append( "WHERE a:EMBEDDED AND length(keys(a))=0 AND NOT((a)<-[*]-(:EMBEDDED)-->())  AND NOT ((a)<-[*]-(x)<-[*]-(b)) AND length(keys(b))>0 ");
					queryBuilder.append( "FOREACH (r in relationships(path) | DELETE r) ");
					queryBuilder.append( "FOREACH (n in nodes(path) | DELETE n) ");
					queryBuilder.append( "WITH a ");
					queryBuilder.append( "MATCH (a)<-[arel]-() ");
					queryBuilder.append( "WHERE length(keys(a))=0 AND a:EMBEDDED ");
					queryBuilder.append( "DELETE arel, a ");
					removeColumn.put( column, queryBuilder.toString() );
				}
			}
		}
		return Collections.unmodifiableMap( removeColumn );
	}

	private Map<String, String> initUpdateToOneQuery(EntityKeyMetadata ownerEntityKeyMetadata, TupleContext tupleContext) {
		if (tupleContext != null) {
			Map<String, AssociatedEntityKeyMetadata> allAssociatedEntityKeyMetadata = tupleContext.getAllAssociatedEntityKeyMetadata();
			Map<String, String> queries = new HashMap<>( allAssociatedEntityKeyMetadata.size() );
			for ( Entry<String, AssociatedEntityKeyMetadata> entry : allAssociatedEntityKeyMetadata.entrySet() ) {
				String associationRole = tupleContext.getRole( entry.getKey() );
				AssociatedEntityKeyMetadata associatedEntityKeyMetadata = entry.getValue();
				EntityKeyMetadata targetKeyMetadata = associatedEntityKeyMetadata.getEntityKeyMetadata();
				StringBuilder queryBuilder = new StringBuilder( "MATCH " );
				appendEntityNode( "owner", ownerEntityKeyMetadata, queryBuilder );
				queryBuilder.append( ", " );
				appendEntityNode( "target", targetKeyMetadata, queryBuilder, ownerEntityKeyMetadata.getColumnNames().length );
				queryBuilder.append( " OPTIONAL MATCH (owner)" );
				queryBuilder.append( " -[r:" );
				appendRelationshipType( queryBuilder, associationRole );
				queryBuilder.append( "]-> () DELETE r " );
				queryBuilder.append( "CREATE (owner) -[:" );
				appendRelationshipType( queryBuilder, associationRole );
				queryBuilder.append( "]-> (target)" );
				queries.put( associationRole, queryBuilder.toString() );
			}
			return queries;
		}
		return Collections.emptyMap();
	}

	private Map<String, String> initFindAssociatedEntityQuery(EntityKeyMetadata ownerEntityKeyMetadata, TupleContext tupleContext) {
		if ( tupleContext != null ) {
			Map<String, AssociatedEntityKeyMetadata> allAssociatedEntityKeyMetadata = tupleContext.getAllAssociatedEntityKeyMetadata();
			Map<String, String> queries = new HashMap<>( allAssociatedEntityKeyMetadata.size() );
			for ( Entry<String, AssociatedEntityKeyMetadata> entry : allAssociatedEntityKeyMetadata.entrySet() ) {
				String associationRole = tupleContext.getRole( entry.getKey() );
				StringBuilder queryBuilder = new StringBuilder( "MATCH " );
				appendEntityNode( "owner", ownerEntityKeyMetadata, queryBuilder );
				queryBuilder.append( " -[r:" );
				appendRelationshipType( queryBuilder, associationRole );
				queryBuilder.append( "]-> (target)" );
				queryBuilder.append( "RETURN target" );
				queries.put( associationRole, queryBuilder.toString() );
			}
			return queries;
		}
		return Collections.emptyMap();
	}

	private Map<String, String> initFindEmbeddedNodeQuery(EntityKeyMetadata ownerEntityKeyMetadata, TupleContext tupleContext) {
		if ( tupleContext != null ) {
			Map<String, String> queries = new HashMap<>();
			List<String> selectableColumns = tupleContext.getSelectableColumns();
			for ( String column : selectableColumns ) {
				if ( isPartOfEmbedded( column ) ) {
					String embeddedPath = column.substring( 0, column.lastIndexOf( "." ) );
					if ( !queries.containsKey( column ) ) {
						String[] columnPath = EmbeddedHelper.split( column );
						StringBuilder queryBuilder = new StringBuilder( "MATCH " );
						appendEntityNode( "owner", ownerEntityKeyMetadata, queryBuilder );
						for ( int i = 0; i < columnPath.length - 1; i++ ) {
							queryBuilder.append( " -[:" );
							appendRelationshipType( queryBuilder, columnPath[i] );
							queryBuilder.append( "]-> (" );
							if ( i == columnPath.length - 2 ) {
								queryBuilder.append( "e" );
							}
							queryBuilder.append( ":" );
							queryBuilder.append( EMBEDDED );
							queryBuilder.append( ")" );
						}
						queryBuilder.append( " RETURN e" );
						queries.put( embeddedPath, queryBuilder.toString() );
					}
				}
			}
			return Collections.unmodifiableMap( queries );
		}
		return Collections.emptyMap();
	}

	/*
	 * Example:
	 * MATCH (owner:ENTITY:table {id: {0}})
	 */
	private static String initMatchOwnerEntityNode(EntityKeyMetadata ownerEntityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder();
		appendMatchOwnerEntityNode( queryBuilder, ownerEntityKeyMetadata );
		return queryBuilder.toString();
	}

	/*
	 * Example:
	 *
	 * MATCH (owner:ENTITY:Car {`carId.maker`: {0}, `carId.model`: {1}}) -[r:tires]- (target)
	 * RETURN r, owner, target
	 *
	 * or for embedded associations:
	 *
	 * MATCH (owner:ENTITY:StoryGame {id: {0}}) -[:evilBranch]-> (:EMBEDDED) -[r:additionalEndings]-> (target:EMBEDDED)
	 * RETURN id(target), r, owner, target ORDER BY id(target)
	 */
	private String completeFindAssociationQuery(String relationshipType) {
		StringBuilder queryBuilder = findAssociationPartialQuery( relationshipType );
		queryBuilder.append( "RETURN id(target), r, owner, target ORDER BY id(target) ");
		return queryBuilder.toString();
	}

	/*
	 * Example:
	 *
	 * MATCH (owner:ENTITY:Car {`carId.maker`: {0}, `carId.model`: {1}}) -[r:tires]- (target)
	 * OPTIONAL MATCH (target) -[x*1..]->(e:EMBEDDED)
	 * RETURN id(target), extract(n IN x| type(n)), x, e ORDER BY id(target)
	 *
	 * or for embedded associations:
	 *
	 * MATCH (owner:ENTITY:StoryGame {id: {0}}) -[:evilBranch]-> (:EMBEDDED) -[r:additionalEndings]-> (target:EMBEDDED)
	 * OPTIONAL MATCH (target) -[x*1..]->(e:EMBEDDED)
	 * RETURN id(target), extract(n IN x| type(n)), x, e ORDER BY id(target)
	 */
	protected String getFindAssociationTargetEmbeddedValues(String relationshipType) {
		StringBuilder queryBuilder = findAssociationPartialQuery( relationshipType );
		queryBuilder.append( "OPTIONAL MATCH (target) -[x*1..]->(e:EMBEDDED) ");
		// Should we split this in two Queries?
		queryBuilder.append( "RETURN id(target), extract(n IN x| type(n)), x, e ORDER BY id(target)");
		return queryBuilder.toString();
	}

	private StringBuilder findAssociationPartialQuery(String relationshipType) {
		StringBuilder queryBuilder = new StringBuilder( findAssociationPartialQuery );
		if ( isPartOfEmbedded( relationshipType ) ) {
			String[] path = split( relationshipType );
			int index = 0;
			for ( String embeddedRelationshipType : path ) {
				queryBuilder.append( " -[" );
				if ( index == path.length - 1 ) {
					queryBuilder.append( "r" );
				}
				queryBuilder.append( ":" );
				appendRelationshipType( queryBuilder, embeddedRelationshipType );
				queryBuilder.append( "]-> (" );
				index++;
				if ( index == path.length ) {
					queryBuilder.append( "target" );
				}
				queryBuilder.append( ":" );
				queryBuilder.append( EMBEDDED );
				queryBuilder.append( ") " );
			}
		}
		else {
			queryBuilder.append( " -[r" );
			queryBuilder.append( ":" );
			appendRelationshipType( queryBuilder, relationshipType );
			queryBuilder.append( "]- (target) " );
		}
		return queryBuilder;
	}

	/*
	 * Example: CREATE (n:EMBEDDED:table {id: {0}}) RETURN n
	 */
	private static String initCreateEmbeddedNodeQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "CREATE " );
		queryBuilder.append( "(n:" );
		queryBuilder.append( EMBEDDED );
		queryBuilder.append( ":" );
		appendLabel( entityKeyMetadata, queryBuilder );
		appendProperties( entityKeyMetadata, queryBuilder );
		queryBuilder.append( ") RETURN n" );
		return queryBuilder.toString();
	}

	/*
	 * This is only the first part of the query, the one related to the owner of the embedded. We need to know the
	 * embedded columns to create the whole query. Example: MERGE (owner:ENTITY:Example {id: {0}}) MERGE (owner)
	 */
	private static String initUpdateEmbeddedNodeQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "MERGE " );
		appendEntityNode( "owner", entityKeyMetadata, queryBuilder );
		queryBuilder.append( " MERGE (owner)" );
		return queryBuilder.toString();
	}

	/*
	 * Example: MATCH (owner:ENTITY:table {id: {0}}) RETURN owner;
	 */
	private static String initFindEntityQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder();
		appendMatchOwnerEntityNode( queryBuilder, entityKeyMetadata );
		queryBuilder.append( " RETURN owner" );
		return queryBuilder.toString();
	}

	/*
	 * Example: MATCH (n:ENTITY:table ) RETURN n
	 */
	private static String initFindEntitiesQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "MATCH " );
		queryBuilder.append( "(n:" );
		queryBuilder.append( ENTITY );
		queryBuilder.append( ":" );
		appendLabel( entityKeyMetadata, queryBuilder );
		queryBuilder.append( ") RETURN n" );
		return queryBuilder.toString();
	}

	/*
	 * Example: CREATE (n:ENTITY:table {id: {0}}) RETURN n
	 */
	private static String initCreateEntityQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "CREATE " );
		appendEntityNode( "n", entityKeyMetadata, queryBuilder );
		queryBuilder.append( " RETURN n" );
		return queryBuilder.toString();
	}

	/*
	 * Example: CREATE (n:ENTITY:table {props}) RETURN n
	 */
	private static String initCreateEntityWithPropertiesQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "CREATE " );
		queryBuilder.append( "(n:" );
		queryBuilder.append( ENTITY );
		queryBuilder.append( ":" );
		appendLabel( entityKeyMetadata, queryBuilder );
		// TODO: We should not pass a map as parameter as Neo4j cannot cache the query plan for it
		queryBuilder.append( " {props})" );
		queryBuilder.append( " RETURN n" );
		return queryBuilder.toString();
	}

	/*
	 * Example: MATCH (n:ENTITY:table {id: {0}}) OPTIONAL MATCH (n) - [r] - () DELETE n, r
	 */
	private static String initRemoveEntityQuery(EntityKeyMetadata entityKeyMetadata) {
		StringBuilder queryBuilder = new StringBuilder( "MATCH " );
		appendEntityNode( "n", entityKeyMetadata, queryBuilder );
		queryBuilder.append( " OPTIONAL MATCH (n)-[r]->(e:EMBEDDED), path=(e)-[*0..]->(:EMBEDDED) " );
		queryBuilder.append( " DELETE r " );
		queryBuilder.append( " FOREACH (er IN relationships(path) | DELETE er) " );
		queryBuilder.append( " FOREACH (en IN nodes(path) | DELETE en) " );
		queryBuilder.append( " WITH n " );
		queryBuilder.append( " OPTIONAL MATCH (n)-[r]-() " );
		queryBuilder.append( " DELETE r,n " );
		return queryBuilder.toString();
	}

	/*
	 * Example:
	 *
	 * MERGE (owner:ENTITY:Account {login: {0}})
	 * MERGE (owner) - [:homeAddress] -> (e:EMBEDDED)
	 *   ON CREATE SET e.country = {1}
	 *   ON MATCH SET e.country = {2}
	 */
	private String initUpdateEmbeddedColumnQuery(Object[] keyValues, String embeddedColumn) {
		StringBuilder queryBuilder = new StringBuilder( getUpdateEmbeddedNodeQuery() );
		String[] columns = appendEmbeddedNodes( embeddedColumn, queryBuilder );
		queryBuilder.append( " ON CREATE SET e." );
		escapeIdentifier( queryBuilder, columns[columns.length - 1] );
		queryBuilder.append( " = {" );
		queryBuilder.append( keyValues.length );
		queryBuilder.append( "}" );
		queryBuilder.append( " ON MATCH SET e." );
		escapeIdentifier( queryBuilder, columns[columns.length - 1] );
		queryBuilder.append( " = {" );
		queryBuilder.append( keyValues.length + 1 );
		queryBuilder.append( "}" );
		return queryBuilder.toString();
	}

	/*
	 * Given an embedded properties path returns the cypher representation that can be appended to a MERGE or CREATE
	 * query.
	 */
	private static String[] appendEmbeddedNodes(String path, StringBuilder queryBuilder) {
		String[] columns = split( path );
		for ( int i = 0; i < columns.length - 1; i++ ) {
			queryBuilder.append( " - [:" );
			appendRelationshipType( queryBuilder, columns[i] );
			queryBuilder.append( "] ->" );
			if ( i < columns.length - 2 ) {
				queryBuilder.append( " (e" );
				queryBuilder.append( i );
				queryBuilder.append( ":" );
				queryBuilder.append( EMBEDDED );
				queryBuilder.append( ") MERGE (e" );
				queryBuilder.append( i );
				queryBuilder.append( ")" );
			}
		}
		queryBuilder.append( " (e:" );
		queryBuilder.append( EMBEDDED );
		queryBuilder.append( ")" );
		return columns;
	}

	public String getUpdateEmbeddedColumnQuery(Object[] keyValues, String embeddedColumn) {
		String query = updateEmbeddedPropertyQueryCache.get( embeddedColumn );
		if ( query == null ) {
			query = initUpdateEmbeddedColumnQuery( keyValues, embeddedColumn );
			String cached = updateEmbeddedPropertyQueryCache.putIfAbsent( embeddedColumn, query );
			if ( cached != null ) {
				query = cached;
			}
		}
		return query;
	}

	public String getFindAssociationQuery(String role) {
		String query = findAssociationQueryCache.get( role );
		if ( query == null ) {
			query = completeFindAssociationQuery( role );
			String cached = findAssociationQueryCache.putIfAbsent( role, query );
			if ( cached != null ) {
				query = cached;
			}
		}
		return query;
	}

	public String getUpdateEntityPropertiesQuery( Map<String, Object> properties ) {
		StringBuilder queryBuilder = new StringBuilder( updateEntityProperties );
		queryBuilder.append( " SET " );
		int index = entityKeyMetadata.getColumnNames().length;
		for ( Map.Entry<String, Object> entry : properties.entrySet() ) {
			queryBuilder.append( "owner." );
			escapeIdentifier( queryBuilder, entry.getKey() );
			queryBuilder.append( " = {" );
			queryBuilder.append( index );
			queryBuilder.append( "}, " );
			index++;
		}
		return queryBuilder.substring( 0, queryBuilder.length() - 2 );
	}

	public String getCreateEmbeddedNodeQuery() {
		return createEmbeddedNodeQuery;
	}

	public String getFindEntityQuery() {
		return findEntityQuery;
	}

	public String getFindEntitiesQuery() {
		return findEntitiesQuery;
	}

	public String getFindAssociationPartialQuery() {
		return findAssociationPartialQuery;
	}

	public String getCreateEntityQuery() {
		return createEntityQuery;
	}

	public String getCreateEntityWithPropertiesQuery() {
		return createEntityWithPropertiesQuery;
	}

	public String getRemoveEntityQuery() {
		return removeEntityQuery;
	}

	public String getUpdateEmbeddedNodeQuery() {
		return updateEmbeddedNodeQuery;
	}

	public String getUpdateToOneQuery(String associationRole) {
		return updateToOneQuery.get( associationRole );
	}

	public String getFindAssociatedEntityQuery(String associationRole) {
		return findAssociatedEntityQuery.get( associationRole );
	}

	public String getRemoveColumnQuery(String column) {
		return removePropertyQueries.get( column );
	}

	public Map<String, String> getFindEmbeddedNodeQueries() {
		return findEmbeddedNodeQueries;
	}

	public Map<String, String> getRemoveEmbeddedPropertyQuery() {
		return removeEmbeddedPropertyQuery;
	}

	public Map<String, String> getRemovePropertyQueries() {
		return removePropertyQueries;
	}

	public String getRemoveToOneAssociation() {
		return removeToOneAssociation;
	}

	public String getUpdateEntityProperties() {
		return updateEntityProperties;
	}

	public Map<String, String> getFindAssociatedEntityQuery() {
		return findAssociatedEntityQuery;
	}
}
