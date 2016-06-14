/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.jpa;

import java.io.Serializable;
import java.util.Objects;

import javax.persistence.Embeddable;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
// @Entity
public class PhoneNumber {

	// @EmbeddedId
	private PhoneNumberId id;
	private String description;

	public PhoneNumber() {
	}

	public PhoneNumber(PhoneNumberId id, String description) {
		this.id = id;
		this.description = description;
	}

	public PhoneNumberId getId() {
		return id;
	}

	public void setId(PhoneNumberId id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Embeddable
	public static class PhoneNumberId implements Serializable {

		private static final long serialVersionUID = 1L;
		private String countryCode;
		private long number;

		public PhoneNumberId() {
		}

		public PhoneNumberId(String countryCode, long number) {
			this.countryCode = countryCode;
			this.number = number;
		}

		public String getCountryCode() {
			return countryCode;
		}

		public void setCountryCode(String countryCode) {
			this.countryCode = countryCode;
		}

		public long getNumber() {
			return number;
		}

		public void setNumber(long number) {
			this.number = number;
		}

		@Override
		public int hashCode() {
			int hash = 3;
			hash = 59 * hash + Objects.hashCode( this.countryCode );
			hash = 59 * hash + (int) ( this.number ^ ( this.number >>> 32 ) );
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			if ( obj == null ) {
				return false;
			}
			if ( getClass() != obj.getClass() ) {
				return false;
			}
			final PhoneNumberId other = (PhoneNumberId) obj;
			if ( this.number != other.number ) {
				return false;
			}
			if ( !Objects.equals( this.countryCode, other.countryCode ) ) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "PhoneNumberId [countryCode=" + countryCode + ", number=" + number + "]";
		}
	}

}
