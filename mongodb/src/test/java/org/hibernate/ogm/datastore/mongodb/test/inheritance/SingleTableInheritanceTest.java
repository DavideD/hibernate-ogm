/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.inheritance;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import javax.persistence.EntityManager;

import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Davide D'Alto
 */
public class SingleTableInheritanceTest extends OgmJpaTestCase {

	Node parent;
	Node simpleChild;
	TextNode textChild;
	NodeLink linkToSimple;
	NodeLink linkToText;

	@Before
	public void setUp() {
		EntityManager em = getFactory().createEntityManager();
		try {
			em.getTransaction().begin();

			parent = new SimpleNode();
			parent.setName( "root" );

			simpleChild = new SimpleNode();
			simpleChild.setName( "children 1" );

			textChild = new TextNode();
			textChild.setName( "children 2" );
			textChild.setText( "a text" );

			linkToSimple = new NodeLink();
			linkToSimple.assignSource( parent );
			linkToSimple.assignTarget( simpleChild );

			linkToText = new NodeLink();
			linkToText.assignSource( parent );
			linkToText.assignTarget( textChild );

			em.persist( parent );
			em.persist( simpleChild );
			em.persist( textChild );
			em.persist( linkToText );
			em.persist( linkToSimple );

			em.getTransaction().commit();
		}
		finally {
			em.close();
		}
	}

	@Test
	public void testSubclassReturnedByNativeQuery() throws Exception {
		EntityManager em = getFactory().createEntityManager();
		try {
			em.getTransaction().begin();
			@SuppressWarnings("unchecked")
			List<TextNode> resultList = em.createNativeQuery( "db.Node.find({'DTYPE': 'TextNode'})", TextNode.class ).getResultList();
			assertThat( resultList ).containsExactly( textChild );
			assertThat( ( (TextNode) resultList.get( 0 ) ).getText() ).isEqualTo( textChild.getText() );
			em.getTransaction().commit();
		}
		finally {
			em.close();
		}
	}

	@Test
	public void testPolymorphimWithNativeQuery() throws Exception {
		EntityManager em = getFactory().createEntityManager();
		try {
			em.getTransaction().begin();
			@SuppressWarnings("unchecked")
			List<SimpleNode> resultList = em.createNativeQuery( "{ $query : { DTYPE: 'SimpleNode' }, $orderby : { name : 1 } }", SimpleNode.class )
					.getResultList();
			assertThat( resultList ).containsExactly( simpleChild, parent );

			SimpleNode actualSimple = resultList.get( 0 );
			assertThat( actualSimple.getChildren() ).isEmpty();

			SimpleNode actualRoot = resultList.get( 1 );
			assertThat( actualRoot.getChildren() ).containsOnly( linkToSimple, linkToText );
			em.getTransaction().commit();
		}
		finally {
			em.close();
		}
	}

	@After
	public void tearDown() {
		remove( parent, simpleChild, textChild );
	}

	private void remove(Node... nodes) {
		EntityManager em = getFactory().createEntityManager();
		try {
			em.getTransaction().begin();
			for ( Node node : nodes ) {
				Node found = em.find( Node.class, node.getId() );
				em.remove( found );
			}
			em.getTransaction().commit();
		}
		finally {
			em.close();
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[]{ Node.class, SimpleNode.class, TextNode.class, NodeLink.class };
	}
}
