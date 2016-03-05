/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
<<<<<<< HEAD
 * 
=======
 *
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
<<<<<<< HEAD
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKind;
=======

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.spi.TupleSnapshot;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBTupleAssociationSnapshot implements TupleSnapshot {

	private static Log log = LoggerFactory.getLogger();
<<<<<<< HEAD
	private Map<String, Object> relationship;
	private AssociationKey associationKey;
	private AssociationContext associationContext;
	private final Map<String, Object> properties;

	public OrientDBTupleAssociationSnapshot(Map<String, Object> relationship, AssociationKey associationKey, AssociationContext associationContext) {
		log.info( "OrientDBTupleAssociationSnapshot: AssociationKey:" + associationKey + "; AssociationContext" + associationContext );
=======
	private AssociationContext associationContext;
	private AssociationKey associationKey;
	private final Map<String, Object> properties;

	private Map<String, Object> relationship;

	public OrientDBTupleAssociationSnapshot(Map<String, Object> relationship, AssociationKey associationKey, AssociationContext associationContext) {
		log.debug( "OrientDBTupleAssociationSnapshot: AssociationKey:" + associationKey + "; AssociationContext" + associationContext );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
<<<<<<< HEAD
			log.info( "rowKeyColumn: " + rowKeyColumn + ";" );

			for ( int i1 = 0; i1 < associationKey.getColumnNames().length; i1++ ) {
				String columnName = associationKey.getColumnNames()[i1];
				log.info( "columnName: " + columnName + ";" );
				if ( rowKeyColumn.equals( columnName ) ) {
					log.info( "column value : " + associationKey.getColumnValue( columnName ) + ";" );
=======
			log.debug( "rowKeyColumn: " + rowKeyColumn + ";" );

			for ( int i1 = 0; i1 < associationKey.getColumnNames().length; i1++ ) {
				String columnName = associationKey.getColumnNames()[i1];
				log.debug( "columnName: " + columnName + ";" );
				if ( rowKeyColumn.equals( columnName ) ) {
					log.debug( "column value : " + associationKey.getColumnValue( columnName ) + ";" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
					properties.put( rowKeyColumn, associationKey.getColumnValue( columnName ) );
				}
			}

		}
		properties.putAll( relationship );

<<<<<<< HEAD
		log.info( "1.collectProperties: " + properties );
=======
		log.debug( "1.collectProperties: " + properties );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

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
<<<<<<< HEAD
		log.info( "collectProperties: " + properties );
		return properties;
	}

	private static boolean isEmbeddedCollection(AssociationKey associationKey) {
		return associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION;
	}

	@Override
	public Object get(String column) {
		log.info( "targetColumnName: " + column );
=======
		log.debug( "collectProperties: " + properties );
		return properties;
	}

	@Override
	public Object get(String column) {
		log.debug( "targetColumnName: " + column );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		return properties.get( column );
	}

	@Override
<<<<<<< HEAD
	public boolean isEmpty() {
		log.info( "isEmpty " );
		return properties.isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		log.info( "getColumnNames " );
		return properties.keySet();
=======
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
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}

}
