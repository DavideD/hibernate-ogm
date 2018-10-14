/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.gridfs;

import static org.hibernate.ogm.datastore.mongodb.options.BinaryStorageType.GRID_FS;

import java.sql.Blob;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;

import org.hibernate.ogm.datastore.mongodb.options.BinaryStorage;
import org.hibernate.ogm.datastore.mongodb.options.GridFSBucket;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */

@Entity
public class Photo {

	public static final String BUCKET_NAME = "photos";

	@Id
	private String id;

	@Lob
	@GridFSBucket(BUCKET_NAME)
	@BinaryStorage(GRID_FS)
	private Blob contentAsBlob;

	@Lob
	@GridFSBucket(BUCKET_NAME)
	@BinaryStorage(GRID_FS)
	private byte[] contentAsByteArray;

	@Lob
	@GridFSBucket(BUCKET_NAME)
	@BinaryStorage(GRID_FS)
	private String contentAsString;

	public Photo() {
	}

	public Photo(String id) {
		this.id = id;
	}

	public Photo(String id, Blob contentAsBlob) {
		this.id = id;
		this.contentAsBlob = contentAsBlob;
	}

	public Photo(String id, String contentAsString) {
		this.id = id;
		this.contentAsString = contentAsString;
	}

	public Photo(String id, byte[] contentAsByteArray) {
		this.id = id;
		this.contentAsByteArray = contentAsByteArray;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Blob getContentAsBlob() {
		return contentAsBlob;
	}

	public void setContentAsBlob(Blob contentAsBlob) {
		this.contentAsBlob = contentAsBlob;
	}

	public byte[] getContentAsByteArray() {
		return contentAsByteArray;
	}

	public void setContentAsByteArray(byte[] contentAsByteArray) {
		this.contentAsByteArray = contentAsByteArray;
	}

	public String getContentAsString() {
		return contentAsString;
	}

	public void setContentAsString(String contentAsString) {
		this.contentAsString = contentAsString;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		Photo photo = (Photo) o;
		return Objects.equals( id, photo.id );
	}

	@Override
	public int hashCode() {
		return Objects.hash( id );
	}

	@Override
	public String toString() {
		return "Photo [id=" + id + ", contentAsString=" + contentAsString + "]";
	}
}
