/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.inheritance.tableperclass.forum;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;

import org.hibernate.search.annotations.Indexed;

@Entity
@Indexed
public class EntityB extends EntityA {

	@ManyToOne
	private EntityA parent;

	public EntityA getParent() {
		return parent;
	}

	public void setParent(EntityA parent) {
		this.parent = parent;
	}
}
