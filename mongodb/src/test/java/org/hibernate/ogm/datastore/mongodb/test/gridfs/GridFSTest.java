/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.gridfs;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.datastore.mongodb.test.gridfs.Photo.BUCKET_NAME;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import javax.persistence.EntityManager;

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.ogm.datastore.mongodb.impl.MongoDBDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.hibernatecore.impl.OgmSessionFactoryImpl;
import org.hibernate.ogm.utils.TestForIssue;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.After;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * @author Davide D'Alto
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 * @see <a href="http://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/gridfs/">GridFSBucket</a>
 */
@TestForIssue(jiraKey = "OGM-786")
public class GridFSTest extends OgmJpaTestCase {

	// GridFS is usually for file of size bigger than 16 MB
	private static final int BLOB_SIZE = 30; // * 1024 * 1024; // 30 MB

	private static final byte[] BLOB_CONTENT_1 = createString( 'b', BLOB_SIZE ).getBytes();
	private static final byte[] BLOB_CONTENT_2 = createString( '8', BLOB_SIZE ).getBytes();

	private static final String STRING_CONTENT_1 = createString( 'a', 1 );
	private static final String STRING_CONTENT_2 = createString( 'x', 1 );

	private static final byte[] BYTE_ARRAY_CONTENT_1 = STRING_CONTENT_1.getBytes();
	private static final byte[] BYTE_ARRAY_CONTENT_2 = STRING_CONTENT_2.getBytes();

	private static final String ENTITY_ID_1 = "photo1";
	private static final String ENTITY_ID_2 = "photo2";

	private static String createString(char c, int size) {
		char[] chars = new char[size];
		Arrays.fill( chars, c );
		return new String( chars );
	}

	@After
	public void deleteAll() throws Exception {
		removeEntities();
	}

	@Test
	public void testGridFSCreationForString() {
		inTransaction( em -> {
			Photo photo = new Photo( ENTITY_ID_1, STRING_CONTENT_1 );
			em.persist( photo );
		} );

		Function<ByteArrayOutputStream, String> function = (ByteArrayOutputStream outputStream) -> {
			return new String( outputStream.toByteArray(), StandardCharsets.UTF_8 );
		};
		List<String> bucketContent = bucketContent( BUCKET_NAME, function );

		assertThat( bucketContent ).containsExactly( STRING_CONTENT_1 );
	}

	@Test
	public void testGridFSCreationForByteArray() {
		inTransaction( em -> {
			Photo photo = new Photo( ENTITY_ID_1, BYTE_ARRAY_CONTENT_1 );
			em.persist( photo );
		} );

		Function<ByteArrayOutputStream, byte[]> function = ( ByteArrayOutputStream outputStream ) -> {
			return outputStream.toByteArray();
		};
		List<?> bucketContent = bucketContent( BUCKET_NAME, function );

		// Normally I would use containsExactly, but it fails for some reason
		assertThat( bucketContent ).hasSize( 1 );
		assertThat( bucketContent.get( 0 ) ).isEqualTo( BYTE_ARRAY_CONTENT_1 );
	}

	@Test
	public void testGridFSCreationForBlob() {
		inTransaction( em -> {
			Blob blob = createBlob( em, BLOB_CONTENT_1 );
			Photo photo = new Photo( ENTITY_ID_1, blob );
			em.persist( photo );
		} );

		Function<ByteArrayOutputStream, byte[]> function = (ByteArrayOutputStream outputStream) -> {
			return outputStream.toByteArray();
		};
		List<byte[]> bucketContent = bucketContent( BUCKET_NAME, function );

		// Normally I would use containsExactly, but it fails for some reason
		assertThat( bucketContent ).hasSize( 1 );
		assertThat( bucketContent.get( 0 ) ).isEqualTo( BLOB_CONTENT_1 );
	}

	@Test
	public void testSavingDifferentFields() {
		inTransaction( em -> {
			Photo photo1 = new Photo( ENTITY_ID_1 );
			photo1.setContentAsBlob( createBlob( em, BLOB_CONTENT_1 ) );
			photo1.setContentAsByteArray( BYTE_ARRAY_CONTENT_1 );
			photo1.setContentAsString( STRING_CONTENT_1 );

			Photo photo2 = new Photo( ENTITY_ID_2 );
			photo2.setContentAsBlob( createBlob( em, BLOB_CONTENT_2 ) );
			photo2.setContentAsByteArray( BYTE_ARRAY_CONTENT_2 );
			photo2.setContentAsString( STRING_CONTENT_2 );
			em.persist( photo2 );
		} );

		inTransaction( em -> {
			Photo photo1 = em.find( Photo.class, ENTITY_ID_1 );
			assertThat( photo1 ).isNotNull();
			assertThat( photo1.getContentAsString() ).isEqualTo( STRING_CONTENT_1 );
			assertThat( photo1.getContentAsByteArray() ).isEqualTo( BYTE_ARRAY_CONTENT_1 );
			assertThatBlobsAreEqual( photo1.getContentAsBlob(), BLOB_CONTENT_1 );

			Photo photo2 = em.find( Photo.class, ENTITY_ID_2 );
			assertThat( photo2 ).isNotNull();
			assertThat( photo2.getContentAsString() ).isEqualTo( STRING_CONTENT_2 );
			assertThat( photo2.getContentAsByteArray() ).isEqualTo( BYTE_ARRAY_CONTENT_2 );
			assertThatBlobsAreEqual( photo2.getContentAsBlob(), BLOB_CONTENT_2 );
		} );
	}

	@Test
	public void testStringEntityFieldIsSaved() {
		inTransaction( em -> {
			Photo photo = new Photo( ENTITY_ID_1, STRING_CONTENT_1 );
			em.persist( photo );
		} );

		inTransaction( em -> {
			Photo photo = em.find( Photo.class, ENTITY_ID_1 );
			assertThat( photo ).isNotNull();
			assertThat( photo.getContentAsString() ).isEqualTo( STRING_CONTENT_1 );
		} );
	}

	@Test
	public void testByteArrayEntityFieldIsSaved() {
		inTransaction( em -> {
			Photo photo = new Photo( ENTITY_ID_1, BYTE_ARRAY_CONTENT_1 );
			em.persist( photo );
		} );

		inTransaction( em -> {
			Photo photo = em.find( Photo.class, ENTITY_ID_1 );
			assertThat( photo ).isNotNull();
			assertThat( photo.getContentAsByteArray() ).isEqualTo( BYTE_ARRAY_CONTENT_1 );
		} );
	}

	@Test
	public void testBlobEntityFieldIsSaved() {
		inTransaction( em -> {
			Blob blob = createBlob( em, BLOB_CONTENT_1 );
			Photo photo = new Photo( ENTITY_ID_1, blob );
			em.persist( photo );
		} );

		inTransaction( em -> {
			Photo photo = em.find( Photo.class, ENTITY_ID_1 );
			assertThat( photo ).isNotNull();
			assertThatBlobsAreEqual( photo.getContentAsBlob(), BLOB_CONTENT_1 );
		} );
	}

	private Blob createBlob(EntityManager em, byte[] blobAsBytes) {
		return Hibernate.getLobCreator( em.unwrap( Session.class ) ).createBlob( BLOB_CONTENT_1 );
	}

	// Fails
	@Test
	public void canUpdateBlobInEntityAndBucket() {
		inTransaction( em -> {
			Blob blob = createBlob( em, BLOB_CONTENT_1 );
			Photo photo = new Photo( ENTITY_ID_1, blob );
			em.persist( photo );
		} );

		inTransaction( em -> {
			Blob blob2 = Hibernate.getLobCreator( em.unwrap( Session.class ) ).createBlob( BLOB_CONTENT_2 );

			Photo photo = em.find( Photo.class, ENTITY_ID_1 );
			photo.setContentAsBlob( blob2 );
		} );

		// Check change has been saved
		inTransaction( em -> {
			Photo photo = em.find( Photo.class, ENTITY_ID_1 );

			assertThat( photo ).isNotNull();
			assertThatBlobsAreEqual( photo.getContentAsBlob(), BLOB_CONTENT_2 );
		} );

		// Check GridFS has been updated
		Function<ByteArrayOutputStream, byte[]> function = (ByteArrayOutputStream outputStream) -> {
			return outputStream.toByteArray();
		};
		List<byte[]> bucketContent = bucketContent( BUCKET_NAME, function );

		// Normally I would use containsExactly, but it fails for some reason
		assertThat( bucketContent ).hasSize( 1 );
		assertThat( bucketContent.get( 0 ) ).isEqualTo( BLOB_CONTENT_2 );
	}

	@Test
	public void canRemoveEntityWithBlob() {
		inTransaction( em -> {
			Photo photo = new Photo();
			photo.setId( ENTITY_ID_2 );

			Blob blob = Hibernate.getLobCreator( em.unwrap( Session.class ) ).createBlob( BLOB_CONTENT_1 );
			photo.setContentAsBlob( blob );
			em.persist( photo );
		} );

		inTransaction( em -> {
			Photo photo = em.find( Photo.class, ENTITY_ID_2 );
			em.remove( photo );
		} );

		Function<ByteArrayOutputStream, byte[]> function = (ByteArrayOutputStream outputStream) -> {
			return outputStream.toByteArray();
		};
		List<byte[]> bucketContent = bucketContent( BUCKET_NAME, function );

		assertThat( bucketContent.isEmpty() );
	}

	private <T> List<T> bucketContent(String bucketName, Function<ByteArrayOutputStream, T> function) {
		List<T> bucketContent = new ArrayList<>();
		inTransaction( em -> {
			MongoDatabase mongoDatabase = getCurrentDB( em );
			GridFSBucket gridFSFilesBucket = GridFSBuckets.create( mongoDatabase, bucketName );
			MongoCursor<GridFSFile> cursor = gridFSFilesBucket.find().iterator();
			while ( cursor.hasNext() ) {
				GridFSFile savedFile = cursor.next();
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				gridFSFilesBucket.downloadToStream( savedFile.getObjectId(), outputStream );
				T actual = function.apply( outputStream );
				bucketContent.add( actual );
			}
		} );
		return bucketContent;
	}

	private MongoDatabase getCurrentDB(EntityManager em) {
		Session session = em.unwrap( Session.class );
		OgmSessionFactoryImpl sessionFactory = (OgmSessionFactoryImpl) session.getSessionFactory();
		MongoDBDatastoreProvider mongoDBDatastoreProvider = (MongoDBDatastoreProvider) sessionFactory.getServiceRegistry()
				.getService( DatastoreProvider.class );
		return mongoDBDatastoreProvider.getDatabase();
	}

	private void assertThatBlobsAreEqual(Blob actual, byte[] expected) {
		if ( expected != null ) {
			assertThat( actual ).as( "Expected " + expected + " but value is null" ).isNotNull();
		}
		try ( InputStream binaryStream = actual.getBinaryStream() ) {
			byte[] actualAsByte = new byte[expected.length];
			DataInputStream stream = new DataInputStream( binaryStream );
			stream.readFully( actualAsByte );
			stream.close();
			assertThat( actualAsByte ).isEqualTo( expected );
		}
		catch (Exception e) {
			throw new RuntimeException( e );
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[]{ Photo.class };
	}

}
