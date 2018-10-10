/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.hibernate.ogm.datastore.mongodb.impl.MongoDBDatastoreProvider;
import org.hibernate.ogm.datastore.mongodb.options.BinaryStorageType;
import org.hibernate.ogm.datastore.mongodb.options.impl.BinaryStorageOption;
import org.hibernate.ogm.datastore.mongodb.type.GridFS;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.options.spi.OptionsService;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class BinaryStorageManager {

	private static final BinaryStorage NOOP_DELEGATOR = new NoopBinaryStorage();

	private final Map<String, FieldsWithBinaryStorageOption> tableEntityTypeMapping;

	private final OptionsService optionsService;

	private final BinaryStorage gridFsStorage;

	public BinaryStorageManager(MongoDBDatastoreProvider provider,
			OptionsService optionsService,
			Map<String, FieldsWithBinaryStorageOption> tableEntityTypeMapping) {
		this.optionsService = optionsService;
		this.tableEntityTypeMapping = tableEntityTypeMapping;
		this.gridFsStorage = new GridFSBinaryStore( provider.getDatabase(), optionsService );
	}

	public void storeContentToBinaryStorage(Document currentDocument, EntityKeyMetadata metadata ) {
		if ( currentDocument != null && metadata != null ) {
			for ( String fieldName : currentDocument.keySet() ) {
				if ( fieldName.equals( "$set" ) ) {
					// it is part of request. it is not document
					Document queryFields = (Document) currentDocument.get( fieldName );
					for ( String queryField : queryFields.keySet() ) {
						storeContentFromFieldToBinaryStorage( queryFields, metadata, queryField );
					}
				}
				else {
					// it is not document
					storeContentFromFieldToBinaryStorage( currentDocument, metadata, fieldName );
				}
			}
		}
	}

	private void storeContentFromFieldToBinaryStorage(Document currentDocument, EntityKeyMetadata metadata, String fieldName ) {
		Object fieldValue = currentDocument.get( fieldName );
		if ( fieldValue instanceof GridFS ) {
			gridFsStorage.storeContentToBinaryStorage( null, currentDocument, fieldName );
		}
	}

	public void removeFromBinaryStorageByEntity(Document deletedDocument, EntityKeyMetadata entityKeyMetadata) {
		if ( entityKeyMetadata != null ) {
			FieldsWithBinaryStorageOption storageFields = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
			if ( storageFields != null ) {
				Class<?> entityClass = storageFields.getEntityClass();
				for ( Field currentField : entityClass.getDeclaredFields() ) {
					BinaryStorage binaryStorage = binaryStorage( entityKeyMetadata, currentField.getName() );
					if ( binaryStorage != null ) {
						OptionsContext optionsContext = propertyOptions( entityClass, currentField.getName() );
						binaryStorage.removeContentFromBinaryStore( optionsContext, deletedDocument, currentField.getName() );
					}
				}
			}
		}
	}

	public void loadContentFromBinaryStorage(Document currentDocument, EntityKeyMetadata metadata) {
		if ( metadata != null ) {
			FieldsWithBinaryStorageOption fields = tableEntityTypeMapping.get( metadata.getTable() );
			if ( currentDocument != null && fields != null ) {
				Set<String> sourceKeySet = new HashSet<>( currentDocument.keySet() );
				for ( String fieldName : sourceKeySet ) {
					OptionsContext optionsContext = propertyOptions( fields.getEntityClass(), fieldName );
					BinaryStorage binaryStorage = binaryStorage( optionsContext );
					binaryStorage.loadContentFromBinaryStorageToField( optionsContext, currentDocument, fieldName );
				}
			}
		}
	}

	private OptionsContext propertyOptions(Class<?> entityType, String fieldName) {
		OptionsContext propertyOptions = optionsService.context().getPropertyOptions( entityType, fieldName );
		return propertyOptions;
	}

	private BinaryStorage binaryStorage(EntityKeyMetadata entityKeyMetadata, String fieldName) {
		FieldsWithBinaryStorageOption map = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		if ( map != null ) {
			BinaryStorageType binaryStorageType = map.get( fieldName );
			return binaryStorage( binaryStorageType );
		}
		return null;
	}

	private BinaryStorage binaryStorage(OptionsContext optionsContext) {
		BinaryStorageType binaryStorageType = optionsContext.getUnique( BinaryStorageOption.class );
		return binaryStorage( binaryStorageType );
	}

	private BinaryStorage binaryStorage(BinaryStorageType binaryStorageType) {
		if ( binaryStorageType == null ) {
			return NOOP_DELEGATOR;
		}
		switch ( binaryStorageType ) {
			case GRID_FS:
				return gridFsStorage;
			default:
				return NOOP_DELEGATOR;
		}
	}
}
