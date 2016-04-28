/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.text.SimpleDateFormat;
import java.util.List;

import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public abstract class AbstractQueryGenerator {

	private static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {

		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat( OrientDBConstant.DATETIME_FORMAT );
		}
	};

	protected static ThreadLocal<SimpleDateFormat> getFormatter() {
		return FORMATTER;
	}

	public static class GenerationResult {

		private List<Object> preparedStatementParams;
		private String executionQuery;

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

}
