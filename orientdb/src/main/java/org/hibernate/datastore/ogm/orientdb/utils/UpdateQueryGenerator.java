/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.Tuple;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class UpdateQueryGenerator extends AbstractQueryGenerator {

	private static final Log log = LoggerFactory.getLogger();

	public GenerationResult generate(String tableName, Tuple tuple, EntityKey primaryKey, Integer currentVersion) {
		return generate( tableName, TupleUtil.toMap( tuple ), primaryKey, currentVersion );
	}

	public GenerationResult generate(String tableName, Map<String, Object> valuesMap, EntityKey primaryKey, Integer currentVersion) {
		StringBuilder updateQuery = new StringBuilder( 200 );
		updateQuery.append( "update " ).append( tableName ).append( " set " );

		Map<String, Object> allValuesMap = new LinkedHashMap<>( valuesMap );
		for ( Map.Entry<String, Object> entry : valuesMap.entrySet() ) {
			String fieldName = entry.getKey();
			Object value = entry.getValue();
			// process ODocument
			if ( value instanceof ODocument ) {
				allValuesMap.remove( fieldName );
				allValuesMap.putAll( ODocumentUtil.extractNamesTree( fieldName, (ODocument) value ) );
			}
		}

		LinkedHashSet<String> columnNames = new LinkedHashSet<>( allValuesMap.keySet() );
		columnNames.removeAll( Arrays.asList( primaryKey.getColumnNames() ) );
		columnNames.removeAll( OrientDBConstant.SYSTEM_FIELDS );
		columnNames.removeAll( OrientDBConstant.MAPPING_FIELDS.keySet() );

		for ( String columnName : columnNames ) {
			Object columnValue = allValuesMap.get( columnName );
			log.debugf( " field name: %s; value class: %s", columnName, ( columnValue != null ? columnValue.getClass() : "null" ) );
			updateQuery.append( columnName ).append( "=" );
			if ( columnValue == null ) {
				updateQuery.append( OrientDBConstant.NULL_VALUE );
			}
			else if ( OrientDBConstant.BASE64_TYPES.contains( columnValue.getClass() ) ) {
				updateQuery.append( "\"" );
				if ( columnValue instanceof BigInteger ) {
					updateQuery.append( new String( Base64.encodeBase64( ( (BigInteger) columnValue ).toByteArray() ) ) );
				}
				else if ( columnValue instanceof byte[] ) {
					updateQuery.append( new String( Base64.encodeBase64( (byte[]) columnValue ) ) );
				}
				updateQuery.append( "\"" );
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
				updateQuery.append( "\"" ).append( formattedStr ).append( "\"" );
			}
			else if ( columnValue instanceof String ) {
				updateQuery.append( "\"" ).append( columnValue ).append( "\"" );
			}
			else if ( columnValue instanceof Character ) {
				updateQuery.append( "\"" ).append( ( (Character) columnValue ).charValue() ).append( "\"" );
			}
			else if ( columnValue instanceof ODocument ) {
				updateQuery.append( columnValue );
			}
			else {
				updateQuery.append( columnValue );
			}
			updateQuery.append( "," );
		}
		updateQuery.setCharAt( updateQuery.lastIndexOf( "," ), ' ' );
		updateQuery.append( " where " );
		// @TODO support multi column primary keys
		updateQuery.append( EntityKeyUtil.generatePrimaryKeyPredicate( primaryKey ) );
		// and version protection
		if ( currentVersion != null ) {
			log.debugf( "version of entity : %d", currentVersion );
			updateQuery.append( " AND " ).append( OrientDBConstant.SYSTEM_VERSION ).append( "=" ).append( currentVersion );
		}
		return new GenerationResult( Collections.emptyList(), updateQuery.toString() );
	}

}
