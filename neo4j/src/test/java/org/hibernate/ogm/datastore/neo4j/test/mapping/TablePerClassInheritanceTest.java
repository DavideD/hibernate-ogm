/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.test.mapping;

import static org.hibernate.ogm.datastore.neo4j.dialect.impl.NodeLabel.ENTITY;
import static org.hibernate.ogm.datastore.neo4j.test.dsl.GraphAssertions.node;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.ogm.datastore.neo4j.test.dsl.NodeForGraphAssertions;
import org.hibernate.search.annotations.Indexed;
import org.junit.Before;
import org.junit.Test;

public class TablePerClassInheritanceTest extends Neo4jJpaTestCase {

	final Root root = new Root( "root_id", "Root" );
	final ChildA a = new ChildA( "a_id", "a" );
	final ChildB b = new ChildB( "b_id", "b" );

	@Before
	public void prepareDB() {
		inTransaction( em -> {
			em.persist( a );
			em.persist( b );
			em.persist( root );
		} );
	}

	@Test
	public void testMapping() throws Exception {
		NodeForGraphAssertions rootNode = node( "root", Root.TABLE, ENTITY.name() )
				.property( "id", root.getId() )
				.property( "name", root.getName() );

		NodeForGraphAssertions childANode = node( "childA", Root.TABLE, ChildA.TABLE, ENTITY.name() )
				.property( "id", a.getId() )
				.property( "name", a.getName() );

		NodeForGraphAssertions childBNode = node( "childB", Root.TABLE, ChildB.TABLE, ENTITY.name() )
				.property( "id", b.getId() )
				.property( "name", b.getName() );

		assertThatOnlyTheseNodesExist( rootNode, childANode, childBNode );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[]{ Root.class, ChildA.class, ChildB.class };
	}

	@Entity
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	@Table(name = Root.TABLE)
	public static class Root {

		public static final String TABLE = "ROOT";

		@Id
		private String id;
		private String name;

		@OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "parent")
		private List<ChildA> ebs = new ArrayList<ChildA>();

		public Root() {
		}

		public Root(String id, String name) {
			this.id = id;
			this.name = name;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<ChildA> getEbs() {
			return ebs;
		}

		public void setEbs(List<ChildA> ebs) {
			this.ebs = ebs;
		}
	}

	@Entity
	@Indexed
	@Table(name = ChildA.TABLE)
	public static class ChildA extends Root {

		public static final String TABLE = "CHILD_A";

		@ManyToOne
		private Root parent;

		public ChildA() {
		}

		public ChildA(String id, String name) {
			super( id, name );
		}

		public Root getParent() {
			return parent;
		}

		public void setParent(Root parent) {
			this.parent = parent;
		}
	}

	@Entity
	@Indexed
	@Table(name = ChildB.TABLE)
	public static class ChildB extends Root {

		public static final String TABLE = "CHILD_B";

		public ChildB() {
		}

		public ChildB(String id, String name) {
			super( id, name );
		}
	}
}
