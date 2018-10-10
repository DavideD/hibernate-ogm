/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Blob;

import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.hibernate.ogm.datastore.mongodb.logging.impl.Log;
import org.hibernate.ogm.datastore.mongodb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.mongodb.options.impl.GridFSBucketOption;
import org.hibernate.ogm.datastore.mongodb.type.GridFS;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.options.spi.OptionsService;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

/**
 * The implementation provides features for GridFS usage
 *
 * @see <a href ="https://docs.mongodb.com/manual/core/gridfs">GridFS documentation</a>
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class GridFSBinaryStore implements BinaryStorage {

	private static final Log log = LoggerFactory.make( MethodHandles.lookup() );

	private static final String CLASS_METADATA_FIELD = "className";

	private final MongoDatabase mongoDatabase;

	public GridFSBinaryStore(MongoDatabase mongoDatabase, OptionsService optionsService) {
		this.mongoDatabase = mongoDatabase;
	}

	@Override
	public void storeContentToBinaryStorage(OptionsContext optionsContext, Document documentToInsert, String fieldName) {
		GridFS binaryContentObject = documentToInsert.get( fieldName, GridFS.class);
		GridFSUploadOptions gridFSUploadOptions = createUploadOptions( binaryContentObject );
		GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, binaryContentObject.getBucketName() );
		ObjectId uploadId = gridFSFilesBucket.uploadFromStream( fieldName, binaryContentObject.getInputStream(), gridFSUploadOptions );
		documentToInsert.put( fieldName, uploadId );
	}

	private GridFSUploadOptions createUploadOptions(Object object) {
		Document metadata = new Document();
		metadata.put( CLASS_METADATA_FIELD, object.getClass().getCanonicalName() );

		GridFSUploadOptions gridFSUploadOptions = new GridFSUploadOptions();
		gridFSUploadOptions.metadata( metadata );
		return gridFSUploadOptions;
	}

	@Override
	public void removeContentFromBinaryStore(OptionsContext optionsContext, Document deletedDocument, String fieldName) {
		String gridfsBucketName = optionsContext.getUnique( GridFSBucketOption.class );
		GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, gridfsBucketName );
		if ( gridFSFilesBucket != null ) {
			ObjectId gridFsLink = deletedDocument.get( fieldName, ObjectId.class );
			if ( gridFsLink != null ) {
				gridFSFilesBucket.delete( gridFsLink );
			}
		}
	}

	@Override
	public void loadContentFromBinaryStorageToField(OptionsContext optionsContext, Document currentDocument, String fieldName) {
		String gridfsBucketName = optionsContext.getUnique( GridFSBucketOption.class );

		GridFSBucket gridFSFilesBucket = getGridFSFilesBucket( mongoDatabase, gridfsBucketName );
		ObjectId uploadId = currentDocument.get( fieldName, ObjectId.class );

		Document metadata = gridFSFilesBucket.openDownloadStream( uploadId ).getGridFSFile().getMetadata();
		String fieldType = (String) metadata.get( CLASS_METADATA_FIELD );

		if ( fieldType.equals( Blob.class.getCanonicalName() ) ) {
			GridFSDownloadStream gridFSDownloadStream = gridFSFilesBucket.openDownloadStream( uploadId );
			currentDocument.put( fieldName, gridFSDownloadStream );
		}
		else if ( fieldType.equals( byte[].class.getCanonicalName() ) || fieldType.equals( BsonBinary.class.getName() )) {
			ByteArrayOutputStream byteArrayContentStream = new ByteArrayOutputStream();
			gridFSFilesBucket.downloadToStream( uploadId, byteArrayContentStream );
			currentDocument.put( fieldName, new Binary( byteArrayContentStream.toByteArray() ) );
		}
		else if ( fieldType.equals( String.class.getCanonicalName() ) ) {
			ByteArrayOutputStream byteArrayContentStream = new ByteArrayOutputStream();
			gridFSFilesBucket.downloadToStream( uploadId, byteArrayContentStream );
			currentDocument.put( fieldName, new String( byteArrayContentStream.toByteArray() ) );
		}
		else {
			throw new RuntimeException("");
		}
	}

	private GridFSBucket getGridFSFilesBucket(MongoDatabase mongoDatabase, String gridfsBucketName) {
		return gridfsBucketName != null
				? GridFSBuckets.create( mongoDatabase, gridfsBucketName )
				: GridFSBuckets.create( mongoDatabase );
	}
}
