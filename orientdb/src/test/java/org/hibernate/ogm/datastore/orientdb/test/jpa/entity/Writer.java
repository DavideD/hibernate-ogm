/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.test.jpa.entity;

import com.orientechnologies.orient.core.id.ORecordId;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
@Entity
public class Writer implements Serializable {

	@Id
	private Long bKey;
	@Version
	@Column(name = "@version")
	private int version;
	@Column(name = "@rid")
	private ORecordId rid;
	@Temporal(TemporalType.DATE)
	private Date birthDate;
	@Convert(converter = StringToUpperCaseConverter.class)
	private String name;
	private long count;

	public Long getbKey() {
		return bKey;
	}

	public void setbKey(Long bKey) {
		this.bKey = bKey;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public ORecordId getRid() {
		return rid;
	}

	public void setRid(ORecordId rid) {
		this.rid = rid;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 29 * hash + Objects.hashCode( this.bKey );
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
		final Writer other = (Writer) obj;
		if ( !Objects.equals( this.bKey, other.bKey ) ) {
			return false;
		}
		return true;
	}

}
