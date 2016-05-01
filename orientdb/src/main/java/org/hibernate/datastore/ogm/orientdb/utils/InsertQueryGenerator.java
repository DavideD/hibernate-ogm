/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.spi.Tuple;
import org.json.simple.JSONObject;

/**
 * Util class for generate query for insert entity
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class InsertQueryGenerator extends AbstractQueryGenerator {

	private static final Log log = LoggerFactory.getLogger();

	public GenerationResult generate(String tableName, Tuple tuple, boolean isStoreTuple, Set<String> keyColumnNames) {
		return generate( tableName, TupleUtil.toMap( tuple ), isStoreTuple, keyColumnNames );
	}

	public GenerationResult generate(String tableName, Map<String, Object> valuesMap, boolean isStoreTuple, Set<String> keyColumnNames) {
		QueryResult queryResult = createJSON( isStoreTuple, keyColumnNames, valuesMap );
		StringBuilder insertQuery = new StringBuilder( 100 );
		insertQuery.append( "insert into " ).append( tableName ).append( " content " ).append( queryResult.getJson().toJSONString() );
		return new GenerationResult( queryResult.getPreparedStatementParams(), insertQuery.toString() );
	}

	protected QueryResult createJSON(boolean isStoreTuple, Set<String> keyColumnNames, Map<String, Object> valuesMap) {
		QueryResult result = new QueryResult();
		for ( Map.Entry<String, Object> entry : valuesMap.entrySet() ) {
			String columnName = entry.getKey();
			Object columnValue = entry.getValue();
			if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) || OrientDBConstant.MAPPING_FIELDS.containsKey( columnName ) ) {
				continue;
			}
			log.debugf( "createJSON: Column %s; value: %s (class: %s). is primary key: %b ",
					columnName, columnValue, ( columnValue != null ? columnValue.getClass() : null ),
					keyColumnNames.contains( columnName ) );
			if ( EntityKeyUtil.isEmbeddedColumn( columnName ) ) {
				if ( isStoreTuple && keyColumnNames.contains( columnName ) ) {
					// it is primary key column
					columnName = columnName.substring( columnName.indexOf( "." ) + 1 );
					// @TODO check type!!!!
					result.getJson().put( columnName, columnValue );
				}
				else {
					EmbeddedColumnInfo ec = new EmbeddedColumnInfo( columnName );
					if ( !result.getJson().containsKey( ec.getClassNames().get( 0 ) ) ) {
						JSONObject embeddedFieldValue = createDefaultEmbeddedRow( ec.getClassNames().get( 0 ) );
						result.getJson().put( ec.getClassNames().get( 0 ), embeddedFieldValue );
					}
					setJsonValue( result, ec, columnValue );
				}
			}
			else if ( columnValue != null && OrientDBConstant.BASE64_TYPES.contains( columnValue.getClass() ) ) {
				if ( columnValue instanceof BigInteger ) {
					result.getJson().put( columnName, new String( Base64.encodeBase64( ( (BigInteger) columnValue ).toByteArray() ) ) );
				}
				else if ( columnValue instanceof byte[] ) {
					result.getJson().put( columnName, new String( Base64.encodeBase64( (byte[]) columnValue ) ) );
				}
			}
			else if ( columnValue instanceof Date || columnValue instanceof Calendar ) {
				Calendar calendar = null;
				if ( columnValue instanceof Date ) {
					calendar = Calendar.getInstance();
					calendar.setTime( (Date) columnValue );
				}
				else if ( columnValue instanceof Calendar ) {
					calendar = (Calendar) columnValue;
				}
				String formattedStr = ( getFormatter().get() ).format( calendar.getTime() );
				result.getJson().put( columnName, formattedStr );
			}
			else if ( columnValue instanceof Character ) {
				result.getJson().put( columnName, ( (Character) columnValue ).toString() );
			}
			else {
				result.getJson().put( columnName, columnValue );
			}
		}
		return result;
	}

	private void setJsonValue(QueryResult result, EmbeddedColumnInfo ec, Object value) {
		log.debugf( "setJsonValue. EmbeddedColumnInfo: %s", ec );
		JSONObject json = (JSONObject) result.getJson().get( ec.getClassNames().get( 0 ) );
		for ( int i = 1; i < ec.getClassNames().size(); i++ ) {
			log.debugf( "setJsonValue. index: %d; className: %s", i, ec.getClassNames().get( i ) );
			if ( !json.containsKey( ec.getClassNames().get( i ) ) ) {
				json.put( ec.getClassNames().get( i ), createDefaultEmbeddedRow( ec.getClassNames().get( i ) ) );
			}
			json = (JSONObject) json.get( ec.getClassNames().get( i ) );
		}
		json.put( ec.getPropertyName(), value );
	}

	private JSONObject createDefaultEmbeddedRow(String className) {
		JSONObject embeddedFieldValue = new JSONObject();
		embeddedFieldValue.put( "@type", "d" );
		embeddedFieldValue.put( "@class", className );
		return embeddedFieldValue;
	}

	protected class QueryResult {

		private List<Object> preparedStatementParams = new LinkedList<>();
		private JSONObject json = new JSONObject();

		public List<Object> getPreparedStatementParams() {
			return preparedStatementParams;
		}

		public void setPreparedStatementParams(List<Object> preparedStatementParams) {
			this.preparedStatementParams = preparedStatementParams;
		}

		public JSONObject getJson() {
			return json;
		}

		public void setJson(JSONObject json) {
			this.json = json;
		}

	}

}
