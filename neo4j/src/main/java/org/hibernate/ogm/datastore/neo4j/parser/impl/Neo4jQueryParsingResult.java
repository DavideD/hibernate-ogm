/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.parser.impl;

import java.util.List;

public class Neo4jQueryParsingResult {

	private final Class<?> entityType;
	private final Object query;
	private final List<String> projections;

	public Neo4jQueryParsingResult(Class<?> entityType, Object query, List<String> projections) {
		this.entityType = entityType;
		this.query = query;
		this.projections = projections;
	}

	public Class<?> getEntityType() {
		return entityType;
	}

	public Object getQuery() {
		return query;
	}

	public List<String> getProjections() {
		return projections;
	}

	@Override
	public String toString() {
		return "Neo4jQueryParsingResult [entityType=" + entityType + ", query=" + query + ", projections=" + projections + "]";
	}

}
