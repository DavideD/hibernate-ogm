/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.dto.GenerationResult;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.Tuple;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import static org.hibernate.ogm.datastore.orientdb.utils.EntityKeyUtil.setFieldValue;
import org.hibernate.ogm.model.key.spi.AssociationKey;

/**
 * The class is generator of 'update' query.
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see <a href="http://orientdb.com/docs/2.2/SQL-Update.html">Update query in OrientDB</a>
 */

public class UpdateQueryGenerator {

	private static final Log log = LoggerFactory.getLogger();

	/**
	 * generate 'update' query for update association
	 *
	 * @param associationKey key of association
	 * @param tuple tuple
	 * @return result
	 * @see GenerationResult
	 */
	public GenerationResult generate(AssociationKey associationKey, Tuple tuple) {
		Set<String> whereColumnNames = new HashSet<>();
		whereColumnNames.addAll( Arrays.asList( associationKey.getColumnNames() ) );
		whereColumnNames.addAll( Arrays.asList( associationKey.getMetadata().getRowKeyColumnNames() ) );
		whereColumnNames.addAll( Arrays.asList( associationKey.getMetadata().getRowKeyIndexColumnNames() ) );

		log.debugf( "generate: whereColumnNames : %s", whereColumnNames );

		StringBuilder updateQuery = generateMainPart( associationKey.getTable(),
				TupleUtil.toMap( tuple ), whereColumnNames.toArray( new String[1] ) );
		// generate 'where' part
		updateQuery.append( " where " );

		for ( Iterator<String> iterator = whereColumnNames.iterator(); iterator.hasNext(); ) {
			String whereColumnName = iterator.next();
			Object value = tuple.get( whereColumnName );
			updateQuery.append( whereColumnName ).append( "=" );
			setFieldValue( updateQuery, value );
			if ( iterator.hasNext() ) {
				updateQuery.append( " and " );
			}
		}

		return new GenerationResult( Collections.emptyList(), updateQuery.toString() );
	}

	/**
	 * generate 'update' query for update association with support version
	 *
	 * @param className name of OrientDB class
	 * @param tuple tuple
	 * @param primaryKey primary key
	 * @param currentVersion version for update
	 * @return result
	 * @see GenerationResult
	 */
	public GenerationResult generate(String className, Tuple tuple, EntityKey primaryKey, Integer currentVersion) {
		return generate( className, TupleUtil.toMap( tuple ), primaryKey, currentVersion );
	}

	/**
	 * generate 'update' query for update association with support version
	 *
	 * @param className name of OrientDB class
	 * @param valuesMap map with column names and their values
	 * @param primaryKey primary key
	 * @param currentVersion version for update
	 * @return result
	 * @see GenerationResult
	 */

	public GenerationResult generate(String className, Map<String, Object> valuesMap, EntityKey primaryKey, Integer currentVersion) {
		StringBuilder updateQuery = generateMainPart( className, valuesMap, primaryKey.getColumnNames() );

		updateQuery.append( " where " );
		log.debugf( "generate: primaryKey : %s", primaryKey );
		updateQuery.append( EntityKeyUtil.generatePrimaryKeyPredicate( primaryKey ) );
		// and version protection
		if ( currentVersion != null && currentVersion > 0 ) {
			log.debugf( "version of entity : %d", currentVersion );
			updateQuery.append( " AND " ).append( OrientDBConstant.SYSTEM_VERSION ).append( "=" ).append( currentVersion );
		}
		return new GenerationResult( Collections.emptyList(), updateQuery.toString() );
	}

	private StringBuilder generateMainPart(String className, Map<String, Object> valuesMap, String[] primaryKeyColumnNames) {
		StringBuilder updateQuery = new StringBuilder( 200 );
		updateQuery.append( "update " ).append( className ).append( " set " );

		Map<String, Object> allValuesMap = new LinkedHashMap<>( valuesMap.size() );
		for ( Map.Entry<String, Object> entry : valuesMap.entrySet() ) {
			String fieldName = entry.getKey();
			Object value = entry.getValue();
			// process ODocument
			if ( value instanceof ODocument ) {
				allValuesMap.remove( fieldName );
				allValuesMap.putAll( ODocumentUtil.extractNamesTree( fieldName, (ODocument) value ) );
			}
			else {
				allValuesMap.put( fieldName, value );
			}
		}

		LinkedHashSet<String> allColumnNames = new LinkedHashSet<>( allValuesMap.keySet() );
		allColumnNames.removeAll( Arrays.asList( primaryKeyColumnNames ) );
		allColumnNames.removeAll( OrientDBConstant.SYSTEM_FIELDS );
		allColumnNames.removeAll( OrientDBConstant.MAPPING_FIELDS.keySet() );
		log.debugf( " generateMainPart: allColumnNames: %s;", allColumnNames );

		for ( String columnName : allColumnNames ) {
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
				String formattedStr = ( FormatterUtil.getDateTimeFormater().get() ).format( calendar.getTime() );
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
		return updateQuery;
	}

}
