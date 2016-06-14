/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.hibernate.AssertionFailure;

import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.RowKey;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBAssociationQueries extends QueriesBase {

	private static final Log log = LoggerFactory.getLogger();

	private final EntityKeyMetadata ownerEntityKeyMetadata;
	private final AssociationKeyMetadata associationKeyMetadata;

	public OrientDBAssociationQueries(EntityKeyMetadata ownerEntityKeyMetadata, AssociationKeyMetadata associationKeyMetadata) {
		this.ownerEntityKeyMetadata = ownerEntityKeyMetadata;
		this.associationKeyMetadata = associationKeyMetadata;
		log.debugf( "ownerEntityKeyMetadata: %s ;associationKeyMetadata: %s ",
				ownerEntityKeyMetadata, associationKeyMetadata );
	}

	public void removeAssociation(Connection connection, AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "removeAssociation: %s ;associationKey: %s;", associationKey );
		log.debugf( "removeAssociation: AssociationKey: %s ; AssociationContext: %s", associationKey, associationContext );
		log.debugf( "removeAssociation: getAssociationKind: %s", associationKey.getMetadata().getAssociationKind() );
		StringBuilder deleteQuery = null;
		String columnName = null;
		log.debugf( "removeAssociation:%s", associationKey.getMetadata().getAssociationKind() );
		log.debugf( "removeAssociation:getRoleOnMainSide:%s", associationContext.getAssociationTypeContext().getRoleOnMainSide() );
		log.debugf( "removeAssociation:getAssociationType:%s", associationKey.getMetadata().getAssociationType() );
		log.debugf( "removeAssociation:AssociatedEntityKeyMetadata:%s", associationKey.getMetadata().getAssociatedEntityKeyMetadata() );
		switch ( associationKey.getMetadata().getAssociationKind() ) {
			case EMBEDDED_COLLECTION:
				deleteQuery = new StringBuilder( "delete vertex " );
				deleteQuery.append( associationKey.getTable() ).append( " where " );
				columnName = associationKey.getColumnNames()[0];
				deleteQuery.append( columnName ).append( "=" );
				EntityKeyUtil.setFieldValue( deleteQuery, associationKey.getColumnValues()[0] );
				break;
			case ASSOCIATION:
				String tableName = associationKey.getTable();
				log.debugf( "removeAssociation:getColumnNames:%s", Arrays.asList( associationKey.getColumnNames() ) );
				columnName = associationKey.getColumnNames()[0];
				deleteQuery = new StringBuilder( 100 );
				deleteQuery.append( "delete vertex " ).append( tableName ).append( " where " );
				deleteQuery.append( columnName ).append( "=" );
				EntityKeyUtil.setFieldValue( deleteQuery, associationKey.getColumnValues()[0] );
				break;
			default:
				throw new AssertionFailure( "Unrecognized associationKind: " + associationKey.getMetadata().getAssociationKind() );
		}

		log.debugf( "removeAssociation: query: %s ", deleteQuery );
		try {
			PreparedStatement pstmt = connection.prepareStatement( deleteQuery.toString() );
			log.debugf( "removeAssociation:AssociationKey: %s. remove: %s", associationKey, pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( deleteQuery.toString(), e );
		}
	}

	public void removeAssociationRow(Connection connection, AssociationKey associationKey, RowKey rowKey) {
		log.debugf( "removeAssociationRow: associationKey: %s; RowKey:%s ", associationKey, rowKey );
		StringBuilder query = new StringBuilder( 100 );
		query.append( "delete vertex " ).append( associationKey.getTable() ).append( " where " );
		for ( int i = 0; i < rowKey.getColumnNames().length; i++ ) {
			String columnName = rowKey.getColumnNames()[i];
			Object columnValue = rowKey.getColumnValues()[i];
			query.append( columnName ).append( "=" );
			EntityKeyUtil.setFieldValue( query, columnValue );
			if ( i < rowKey.getColumnNames().length - 1 ) {
				query.append( " AND " );
			}
		}
		log.debugf( "removeAssociationRow: delete query: %s; ", query );
		try {
			PreparedStatement pstmt = connection.prepareStatement( query.toString() );
			log.debugf( "removeAssociationRow: deleted rows %d; ", pstmt.executeUpdate() );

		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( query.toString(), sqle );
		}

	}

	public List<Map<String, Object>> findRelationship(Connection connection, AssociationKey associationKey, RowKey rowKey) {
		Map<String, Object> relationshipValues = new LinkedHashMap<>();
		log.debugf( "findRelationship: associationKey: %s", associationKey );
		log.debugf( "findRelationship: row key : %s", rowKey );
		log.debugf( "findRelationship: row key index columns: %d",
				associationKey.getMetadata().getRowKeyIndexColumnNames().length );
		log.debugf( "findRelationship: row key column names: %s",
				Arrays.asList( associationKey.getMetadata().getRowKeyColumnNames() ) );
		if ( associationKey.getMetadata().getRowKeyIndexColumnNames().length > 0 ) {
			String[] indexColumnNames = associationKey.getMetadata().getRowKeyIndexColumnNames();
			for ( int i = 0; i < indexColumnNames.length; i++ ) {
				for ( int j = 0; j < rowKey.getColumnNames().length; j++ ) {
					if ( indexColumnNames[i].equals( rowKey.getColumnNames()[j] ) ) {
						relationshipValues.put( rowKey.getColumnNames()[j], rowKey.getColumnValues()[j] );
					}
				}
			}
		}
		else if ( associationKey.getMetadata().getRowKeyColumnNames().length > 0 ) {
			String[] indexColumnNames = associationKey.getMetadata().getRowKeyColumnNames();
			for ( int i = 0; i < indexColumnNames.length; i++ ) {
				for ( int j = 0; j < rowKey.getColumnNames().length; j++ ) {
					if ( indexColumnNames[i].equals( rowKey.getColumnNames()[j] ) ) {
						relationshipValues.put( rowKey.getColumnNames()[j], rowKey.getColumnValues()[j] );
					}
				}
			}

		}
		else {
			EntityKey entityKey = getEntityKey( associationKey, rowKey );
			for ( int i = 0; i < entityKey.getColumnNames().length; i++ ) {
				relationshipValues.put( entityKey.getColumnNames()[i], entityKey.getColumnValues()[i] );
			}
		}
		for ( String columnName : associationKey.getColumnNames() ) {
			relationshipValues.put( columnName, associationKey.getColumnValue( columnName ) );
		}

		for ( Map.Entry<String, Object> entry : relationshipValues.entrySet() ) {
			String key = entry.getKey();
			Object value = entry.getValue();
			log.debugf( "findRelationship: key %s; value: %s", key, value );
		}
		List<Map<String, Object>> dbValues = new LinkedList<>();
		StringBuilder queryBuilder = new StringBuilder( 100 );
		queryBuilder.append( "SELECT  FROM " ).append( associationKey.getTable() ).append( " WHERE " );
		int index = 0;
		for ( Map.Entry<String, Object> entry : relationshipValues.entrySet() ) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if ( index > 0 ) {
				queryBuilder.append( " AND " );
			}
			queryBuilder.append( key ).append( "=" );
			EntityKeyUtil.setFieldValue( queryBuilder, value );
			index++;
		}
		log.debugf( "findRelationship: queryBuilder: %s", queryBuilder );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( queryBuilder.toString() );
			while ( rs.next() ) {
				ResultSetMetaData metadata = rs.getMetaData();
				Map<String, Object> rowValues = new LinkedHashMap<>();
				for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
					rowValues.put( systemField, rs.getObject( systemField ) );
				}
				for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) {
					int dbFieldNo = i + 1;
					String dbColumnName = metadata.getColumnName( dbFieldNo );
					rowValues.put( dbColumnName, rs.getObject( dbColumnName ) );
				}
				dbValues.add( rowValues );
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( queryBuilder.toString(), sqle );
		}
		log.debugf( "findRelationship: found: %d", dbValues.size() );

		return dbValues;
	}

	private EntityKey getEntityKey(AssociationKey associationKey, RowKey rowKey) {
		String[] associationKeyColumns = associationKey.getMetadata().getAssociatedEntityKeyMetadata().getAssociationKeyColumns();
		Object[] columnValues = new Object[associationKeyColumns.length];
		for ( int i = 0; i < associationKeyColumns.length; i++ ) {
			columnValues[i] = rowKey.getColumnValue( associationKeyColumns[i] );
		}
		EntityKeyMetadata entityKeyMetadata = associationKey.getMetadata().getAssociatedEntityKeyMetadata().getEntityKeyMetadata();
		return new EntityKey( entityKeyMetadata, columnValues );
	}
}
