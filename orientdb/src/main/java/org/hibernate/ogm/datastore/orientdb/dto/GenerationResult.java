/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dto;

import java.util.List;

/**
 * The class is presentation of generation of query
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class GenerationResult {

	/**
	 * List of values of query parameters
	 */
	private List<Object> preparedStatementParams;
	/**
	 * The query
	 */
	private String executionQuery;

	/**
	 * Contractor
	 *
	 * @param preparedStatementParams list of parameter's values
	 * @param executionQuery string presentation of query
	 */
	public GenerationResult(List<Object> preparedStatementParams, String executionQuery) {
		this.preparedStatementParams = preparedStatementParams;
		this.executionQuery = executionQuery;
	}

	public List<Object> getPreparedStatementParams() {
		return preparedStatementParams;
	}

	public String getExecutionQuery() {
		return executionQuery;
	}

}
