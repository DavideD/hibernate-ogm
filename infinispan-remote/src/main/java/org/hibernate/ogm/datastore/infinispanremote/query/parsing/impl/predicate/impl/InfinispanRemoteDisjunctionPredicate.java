/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.infinispanremote.query.parsing.impl.predicate.impl;

import org.hibernate.hql.ast.spi.predicate.DisjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.hql.ast.spi.predicate.Predicate;
import org.hibernate.ogm.datastore.infinispanremote.query.parsing.impl.InfinispanRemoteQueryBuilder;

/**
 * Infinispan remote-based implementation of {@link DisjunctionPredicate}.
 *
 * @author Fabio Massimo Ercoli
 */
public class InfinispanRemoteDisjunctionPredicate extends DisjunctionPredicate<InfinispanRemoteQueryBuilder> implements NegatablePredicate<InfinispanRemoteQueryBuilder> {

	public InfinispanRemoteDisjunctionPredicate() {
	}

	@Override
	public InfinispanRemoteQueryBuilder getQuery() {
		return getBaseQuery( " or " );
	}

	@Override
	public InfinispanRemoteQueryBuilder getNegatedQuery() {
		return getBaseQuery( " and " );
	}

	private InfinispanRemoteQueryBuilder getBaseQuery(final String operator) {
		InfinispanRemoteQueryBuilder builder = new InfinispanRemoteQueryBuilder( "(" );

		int counter = 1;
		for ( Predicate<InfinispanRemoteQueryBuilder> child : children ) {
			builder.append( ( (NegatablePredicate<InfinispanRemoteQueryBuilder>) child ).getNegatedQuery() );
			builder.append( ")" );
			if ( counter++ < children.size() ) {
				builder.append( operator + "(" );
			}
		}
		return builder;
	}
}
