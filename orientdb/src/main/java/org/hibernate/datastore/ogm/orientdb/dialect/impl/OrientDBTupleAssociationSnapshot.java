/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.spi.TupleSnapshot;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */

public class OrientDBTupleAssociationSnapshot implements TupleSnapshot {

	private static Log log = LoggerFactory.getLogger();
	private AssociationContext associationContext;
	private AssociationKey associationKey;
	private final Map<String, Object> properties;

	private Map<String, Object> relationship;

	public OrientDBTupleAssociationSnapshot(Map<String, Object> relationship, AssociationKey associationKey, AssociationContext associationContext) {
		log.debug( "OrientDBTupleAssociationSnapshot: AssociationKey:" + associationKey + "; AssociationContext" + associationContext );
		this.relationship = relationship;
		this.associationKey = associationKey;
		this.associationContext = associationContext;
		properties = collectProperties();
	}

	private Map<String, Object> collectProperties() {
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		String[] rowKeyColumnNames = associationKey.getMetadata().getRowKeyColumnNames();

		// Index columns
		for ( int i = 0; i < rowKeyColumnNames.length; i++ ) {
			String rowKeyColumn = rowKeyColumnNames[i];
			log.debug( "rowKeyColumn: " + rowKeyColumn + ";" );

			for ( int i1 = 0; i1 < associationKey.getColumnNames().length; i1++ ) {
				String columnName = associationKey.getColumnNames()[i1];
				log.debug( "columnName: " + columnName + ";" );
				if ( rowKeyColumn.equals( columnName ) ) {
					log.debug( "column value : " + associationKey.getColumnValue( columnName ) + ";" );
					properties.put( rowKeyColumn, associationKey.getColumnValue( columnName ) );
				}
			}

		}
		properties.putAll( relationship );

		log.debug( "1.collectProperties: " + properties );

		// Properties stored in the target side of the association
		/*
		 * AssociatedEntityKeyMetadata associatedEntityKeyMetadata =
		 * associationContext.getAssociationTypeContext().getAssociatedEntityKeyMetadata(); for ( String
		 * associationColumn : associatedEntityKeyMetadata.getAssociationKeyColumns() ) { String targetColumnName =
		 * associatedEntityKeyMetadata.getCorrespondingEntityKeyColumn( associationColumn ); if (
		 * targetNode.containsField( targetColumnName ) ) { properties.put( associationColumn,
		 * targetNode.getOriginalValue( targetColumnName ) ); } }
		 */

		// Property stored in the owner side of the association
		/*
		 * for ( int i = 0; i < associationKey.getColumnNames().length; i++ ) { if ( ownerNode.containsField(
		 * associationKey.getEntityKey().getColumnNames()[i] ) ) { properties.put( associationKey.getColumnNames()[i],
		 * ownerNode.getOriginalValue(associationKey.getEntityKey().getColumnNames()[i] ) ); } }
		 */
		log.debug( "collectProperties: " + properties );
		return properties;
	}

	@Override
	public Object get(String columnName) {
		log.debugf( "targetColumnName:  %s", columnName );

		Object value = properties.get( columnName );
		if ( value == null && EntityKeyUtil.isEmbeddedColumn( columnName ) ) {
			EmbeddedColumnInfo ec = new EmbeddedColumnInfo( columnName );
			ODocument embeddedContainer = (ODocument) properties.get( ec.getClassNames().get( 0 ) );
			value = embeddedContainer.field( ec.getPropertyName() );
		}
		return value;
	}

	@Override
	public Set<String> getColumnNames() {
		log.debug( "getColumnNames " );
		return properties.keySet();
	}

	@Override
	public boolean isEmpty() {
		log.debug( "isEmpty " );
		return properties.isEmpty();
	}

	private static boolean isEmbeddedCollection(AssociationKey associationKey) {
		return associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION;
	}

}
