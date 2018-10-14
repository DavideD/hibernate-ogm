/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hibernate.ogm.datastore.mongodb.impl.MongoDBDatastoreProvider;
import org.hibernate.ogm.datastore.mongodb.options.impl.GridFSBucketOption;
import org.hibernate.ogm.datastore.mongodb.type.GridFS;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.options.spi.OptionsService;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class GridFSStorageManager {

	private final Map<String, GridFSFields> tableEntityTypeMapping;

	private final OptionsService optionsService;

	private final MongoDatabase mongoDatabase;

	public GridFSStorageManager(MongoDBDatastoreProvider provider,
			OptionsService optionsService,
			Map<String, GridFSFields> tableEntityTypeMapping) {
		this.optionsService = optionsService;
		this.tableEntityTypeMapping = tableEntityTypeMapping;
		this.mongoDatabase = provider.getDatabase();
	}

	public void storeContentToBinaryStorage(Document currentDocument, EntityKeyMetadata metadata, Object documentId ) {
		if ( currentDocument != null && metadata != null ) {
			tableEntityTypeMapping.get( metadata.getTable() )
			for ( String fieldName : currentDocument.keySet() ) {
				if ( fieldName.equals( "$set" ) ) {
					// it is part of request. it is not document
					Document queryFields = (Document) currentDocument.get( fieldName );
					for ( String queryField : queryFields.keySet() ) {
						storeContentFromFieldToBinaryStorage( queryFields, metadata, queryField, documentId );
					}
				}
				else {
					// it is not document
					storeContentFromFieldToBinaryStorage( currentDocument, metadata, fieldName, documentId );
				}
			}
		}
	}

	private void storeContentFromFieldToBinaryStorage(Document documentToInsert, EntityKeyMetadata metadata, String fieldName, Object documentId) {
		Object binaryContentObject = documentToInsert.get( fieldName );
		if ( binaryContentObject instanceof GridFS ) {
			GridFS grdfsObject = (GridFS) binaryContentObject;
			String gridfsBucketName = bucketName( metadata, fieldName );
			GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, gridfsBucketName );
			deleteExistingContent( fieldName, documentId, gridFSFilesBucket );
			ObjectId uploadId = gridFSFilesBucket.uploadFromStream( fileName( fieldName, documentId ), grdfsObject.getInputStream() );
			documentToInsert.put( fieldName, uploadId );
		}
	}

	/*
	 * For updates, we want to delete the previous entry
	 */
	private void deleteExistingContent(String fieldName, Object documentId, GridFSBucket gridFSFilesBucket) {
		GridFSFindIterable results = gridFSFilesBucket.find( Filters.and( Filters.eq( "filename", fileName( fieldName, documentId ) ) ) );
		results.forEach( (GridFSFile file) -> {
			gridFSFilesBucket.delete( file.getId() );
		} );
	}

	private String fileName(String fieldName, Object documentId) {
		return fieldName + "_" + String.valueOf( documentId );
	}

	public void removeFromBinaryStorageByEntity(Document deletedDocument, EntityKeyMetadata entityKeyMetadata) {
		if ( entityKeyMetadata != null ) {
			GridFSFields storageFields = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
			if ( storageFields != null ) {
				Set<Field> fields = storageFields.getFields();
				for ( Field gridfsField : fields ) {
					String gridfsBucketName = bucketName( entityKeyMetadata, gridfsField.getName() );
					GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, gridfsBucketName );
					deleteExistingContent( gridfsField.getName(), deletedDocument.get( "_id" ), gridFSFilesBucket );
				}
			}
		}
	}

	private String bucketName(EntityKeyMetadata entityKeyMetadata, String fieldName) {
		GridFSFields storageFields = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		OptionsContext optionsContext = propertyOptions( storageFields.getEntityClass(), fieldName );
		String gridfsBucketName = optionsContext.getUnique( GridFSBucketOption.class );
		if ( gridfsBucketName == null && entityKeyMetadata != null ) {
			// Default
			gridfsBucketName = entityKeyMetadata.getTable() + "_bucket";
		}
		return gridfsBucketName;
	}

	public void loadContentFromBinaryStorage(Document currentDocument, EntityKeyMetadata metadata) {
		if ( metadata != null ) {
			GridFSFields fields = tableEntityTypeMapping.get( metadata.getTable() );
			if ( currentDocument != null && fields != null ) {
				for ( Field field : fields.getFields() ) {
					OptionsContext optionsContext = propertyOptions( fields.getEntityClass(), field.getName() );
					loadContentFromBinaryStorageToField( optionsContext, currentDocument, field.getName() );
				}
			}
		}
	}

	public void loadContentFromBinaryStorageToField(OptionsContext optionsContext, Document currentDocument, String fieldName) {
		String gridfsBucketName = optionsContext.getUnique( GridFSBucketOption.class );

		GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, gridfsBucketName );
		Object uploadId = currentDocument.get( fieldName );
		if ( uploadId != null ) {
			GridFSDownloadStream gridFSDownloadStream = gridFSFilesBucket.openDownloadStream( (ObjectId) uploadId );
			GridFS value = new GridFS( gridFSDownloadStream );
			currentDocument.put( fieldName, value );
		}
	}

	private OptionsContext propertyOptions(Class<?> entityType, String fieldName) {
		OptionsContext propertyOptions = optionsService.context().getPropertyOptions( entityType, fieldName );
		return propertyOptions;
	}

	private GridFSBucket getGridFSFilesBucket(MongoDatabase mongoDatabase, String bucketName) {
		return bucketName != null
				? GridFSBuckets.create( mongoDatabase, bucketName )
				: GridFSBuckets.create( mongoDatabase );
	}
}
