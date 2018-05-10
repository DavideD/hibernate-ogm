/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.ogm.backendtck.associations.collection.types;

import java.io.Serializable;
import java.util.Objects;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 */
@Entity
public class Runner {

	@EmbeddedId
	private RunnerId runnerId;
	private int age;

	public RunnerId getRunnerId() {
		return runnerId;
	}

	public void setRunnerId(RunnerId runnerId) {
		this.runnerId = runnerId;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public int hashCode() {
		return Objects.hash( runnerId, age );
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
		Runner other = (Runner) obj;
		return Objects.equals( other.runnerId, runnerId ) && Objects.equals( other.age, age );
	}

	public static class RunnerId implements Serializable {

		private String firstname;
		private String lastname;

		public RunnerId() {
		}

		public RunnerId(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		@Override
		public int hashCode() {
			return Objects.hash( firstname, lastname );
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			else if ( obj == null ) {
				return false;
			}
			else if ( getClass() != obj.getClass() ) {
				return false;
			}
			RunnerId other = (RunnerId) obj;
			return Objects.equals( other.firstname, firstname ) && Objects.equals( other.lastname, lastname );
		}
	}
}
