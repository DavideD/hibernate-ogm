/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.impl;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.BigDecimalType;
import org.hibernate.type.BigIntegerType;
import org.hibernate.type.BinaryType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.CalendarDateType;
import org.hibernate.type.CalendarType;
import org.hibernate.type.CharacterType;
import org.hibernate.type.CustomType;
import org.hibernate.type.DateType;
import org.hibernate.type.DoubleType;
import org.hibernate.type.EntityType;
import org.hibernate.type.EnumType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.ManyToOneType;
import org.hibernate.type.MaterializedBlobType;
import org.hibernate.type.MaterializedClobType;
import org.hibernate.type.NumericBooleanType;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.SerializableToBlobType;
import org.hibernate.type.ShortType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.TrueFalseType;
import org.hibernate.type.UUIDBinaryType;
import org.hibernate.type.UrlType;
import org.hibernate.type.YesNoType;
import org.hibernate.usertype.UserType;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBSchemaDefiner extends BaseSchemaDefiner {

	private class EmbedColumnName {

		private String emdeddedFieldName;
		private String mainFieldName;

		public EmbedColumnName(String mainFieldName, String emdeddedFieldName) {
			this.mainFieldName = mainFieldName;
			this.emdeddedFieldName = emdeddedFieldName;
		}

		public String getEmdeddedFieldName() {
			return emdeddedFieldName;
		}

		public String getMainFieldName() {
			return mainFieldName;
		}

	}

	private static final String CREATE_PROPERTY_TEMPLATE = "create property {0}.{1} {2}";
	private static final Log log = LoggerFactory.getLogger();
	private static final Pattern PATTERN = Pattern.compile( "directed([a-zA-Z_0-9])\\.(.+)" );
	private static final Set<Class> RELATIONS_TYPES;
	private static final Map<Class, Class> RETURNED_CLASS_TYPE_MAPPING;

	private static final Set<Class> SEQ_TYPES;

	private static final Map<Class, String> TYPE_MAPPING;

	private OrientDBDatastoreProvider provider;

	static {
		Map<Class, String> map = new HashMap<>();

		map.put( ByteType.class, "byte" );
		map.put( IntegerType.class, "integer" );
		map.put( NumericBooleanType.class, "short" );
		map.put( ShortType.class, "short" );
		map.put( LongType.class, "long" );
		map.put( FloatType.class, "float" );
		map.put( DoubleType.class, "double" );
		map.put( DateType.class, "date" );
		map.put( CalendarDateType.class, "date" );
		map.put( TimestampType.class, "datetime" );
		map.put( CalendarType.class, "datetime" );
		map.put( TimeType.class, "datetime" );

		map.put( BooleanType.class, "boolean" );

		map.put( TrueFalseType.class, "string" );
		map.put( YesNoType.class, "string" );
		map.put( StringType.class, "string" );
		map.put( UrlType.class, "string" );
		map.put( MaterializedClobType.class, "string" );
		map.put( CharacterType.class, "string" );
		map.put( UUIDBinaryType.class, "string" );

		map.put( BinaryType.class, "binary" ); // byte[]
		map.put( MaterializedBlobType.class, "binary" ); // byte[]
		map.put( SerializableToBlobType.class, "binary" ); // byte[]
		map.put( BigIntegerType.class, "binary" );

		map.put( BigDecimalType.class, "decimal" );

		TYPE_MAPPING = Collections.unmodifiableMap( map );

		Map<Class, Class> map1 = new HashMap<>();
		map1.put( Long.class, LongType.class );
		map1.put( Integer.class, IntegerType.class );
		map1.put( String.class, StringType.class );
		RETURNED_CLASS_TYPE_MAPPING = Collections.unmodifiableMap( map1 );

		Set<Class> set1 = new HashSet<>();
		set1.add( IntegerType.class );
		set1.add( LongType.class );
		SEQ_TYPES = Collections.unmodifiableSet( set1 );

		Set<Class> set2 = new HashSet<>();
		set2.add( ManyToOneType.class );
		set2.add( OneToOneType.class );
		RELATIONS_TYPES = Collections.unmodifiableSet( set2 );

	}

	private String createClassQuery(Table table) {
		return MessageFormat.format( "create class {0} extends V", table.getName() );
	}

	private String createEdgeType(String edgeType) {
		return MessageFormat.format( "CREATE CLASS {0} EXTENDS E",
				edgeType );
	}

	private void createEntities(SchemaDefinitionContext context) throws SQLException {
		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			log.debug( "namespace: " + namespace.getName() );
			for ( Table table : namespace.getTables() ) {
				log.debug( "table: " + table );
				log.debug( "tableName: " + table.getName() );
				boolean isMappingTable = isMapingTable( table );
				String classQuery = createClassQuery( table );
				log.debug( "create class query: " + classQuery );
				provider.getConnection().createStatement().execute( classQuery );
				Iterator<Column> columnIterator = table.getColumnIterator();

				while ( columnIterator.hasNext() ) {
					Column column = columnIterator.next();
					log.debug( "relation type: " + column.getValue().getType().getClass() );
					log.debug( "column.getName(): " + column.getName() );
					if ( OrientDBConstant.SYSTEM_FIELDS.contains( column.getName() ) ) {
						continue;
					}
					else if ( RELATIONS_TYPES.contains( column.getValue().getType().getClass() ) ) {
						// @TODO refactor it
						Value value = column.getValue();
						log.debug( "column name:" + column.getName() + "; column.getCanonicalName():" + column.getCanonicalName() );

						if ( isEmbeddedColumn( column ) ) {

						}
						else {
							Class mappedByClass = searchMappedByReturnedClass( context, namespace.getTables(), (EntityType) value.getType(), column );
							String propertyQuery = createValueProperyQuery( table, column, RETURNED_CLASS_TYPE_MAPPING.get( mappedByClass ) );
							log.debug( "create foreign key property query: " + propertyQuery );
							provider.getConnection().createStatement().execute( propertyQuery );
						}
						// @TODO use Links as foreign keys. see http://orientdb.com/docs/last/SQL-Create-Link.html

						// @TODO support fields 'in_' and 'out_' for native queries
						// String mappedByName = searchMappedByName( context, namespace.getTables(), (EntityType)
						// value.getType(), column );
						// log.debug( "create edge query: " + createEdgeType( mappedByName ) );
						// provider.getConnection().createStatement().execute( createEdgeType( mappedByName ) );

					}
					else {
						String propertyQuery = createValueProperyQuery( table, column );
						log.debug( "create property query: " + propertyQuery );
						provider.getConnection().createStatement().execute( propertyQuery );
					}
				}
				if ( !isMappingTable ) {
					PrimaryKey primaryKey = table.getPrimaryKey();
					if ( primaryKey != null ) {
						log.debug( "primaryKey: " + primaryKey );
						for ( String primaryKeyQuery : createPrimaryKey( primaryKey ) ) {
							log.debug( "primary key query: " + primaryKeyQuery );
							provider.getConnection().createStatement().execute( primaryKeyQuery );
						}
					}
					else {
						log.debug( "Table " + table.getName() + " has not primary key" );
					}
				}
			}
		}
		provider.getConnection().createStatement().execute( "CREATE SEQUENCE HIBERNATE_SEQUENCE TYPE ORDERED START 1" );
	}

	private List<String> createPrimaryKey(PrimaryKey primaryKey) {
		List<String> queries = new ArrayList<>( 2 );

		String table = primaryKey.getTable().getName();
		StringBuilder columns = new StringBuilder();
		StringBuilder uniqueIndexQuery = new StringBuilder( 100 );
		uniqueIndexQuery.append( "CREATE INDEX " ).append( table ).append( "_" );
		String firstColumn = primaryKey.getColumn( 0 ).getName();
		for ( Iterator<Column> iterator = primaryKey.getColumnIterator(); iterator.hasNext(); ) {
			Column indexColumn = iterator.next();
			columns.append( indexColumn.getName() ).append( "," );
		}
		columns.setLength( columns.length() - 1 );
		uniqueIndexQuery.append( firstColumn ).append( "_pk ON " ).append( table ).append( " (" ).append( columns ).append( ") UNIQUE" );
		queries.add( uniqueIndexQuery.toString() );

		log.debug( "primaryKey.getColumns().get(0).getValue().getType().getClass(): " + primaryKey.getColumns().get( 0 ).getValue().getType().getClass() );
		if ( primaryKey.getColumns().size() == 1 && SEQ_TYPES.contains( primaryKey.getColumns().get( 0 ).getValue().getType().getClass() ) ) {
			StringBuilder seq = new StringBuilder( 100 );
			seq.append( "CREATE SEQUENCE " );
			seq.append( generateSeqName( primaryKey.getTable().getName(), primaryKey.getColumns().get( 0 ).getName() ) );
			seq.append( " TYPE ORDERED START 1" );
			queries.add( seq.toString() );
		}
		return queries;
	}

	private String createValueProperyQuery(Table table, Column column) {
		SimpleValue simpleValue = (SimpleValue) column.getValue();
		return createValueProperyQuery( table, column, simpleValue.getType().getClass() );
	}

	private String createValueProperyQuery(Table table, Column column, Class targetTypeClass) {

		Value value = column.getValue();
		log.debug( "1.Column " + column.getName() + " :" + targetTypeClass );
		String query = null;

		if ( targetTypeClass.equals( CustomType.class ) ) {
			CustomType type = (CustomType) value.getType();
			log.debug( "2.Column " + column.getName() + " :" + type.getUserType() );
			UserType userType = type.getUserType();
			if ( userType instanceof EnumType ) {
				EnumType enumType = (EnumType) type.getUserType();
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						table.getName(), column.getName(), TYPE_MAPPING.get( enumType.isOrdinal() ? IntegerType.class : StringType.class ) );

			}
			else {
				throw new UnsupportedOperationException( "Unsupported user type: " + userType.getClass() );
			}
		}
		else {
			String orientDbTypeName = TYPE_MAPPING.get( targetTypeClass );
			if ( orientDbTypeName == null ) {
				throw new UnsupportedOperationException( "Unsupported type: " + targetTypeClass );
			}
			query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
					table.getName(), column.getName(), orientDbTypeName );
		}
		return query;

	}

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		SessionFactoryImplementor sessionFactoryImplementor = context.getSessionFactory();
		ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();
		provider = (OrientDBDatastoreProvider) registry.getService( DatastoreProvider.class );
		try {
			createEntities( context );
		}
		catch (SQLException e) {
			log.error( "Can not initialize schema!", e );
			throw new RuntimeException( "Can not initialize schema!", e );
		}
	}

	private boolean isEmbeddedColumn(Column column) {
		return column.getValue().getType().getClass().equals( ManyToOneType.class ) && column.getName().contains( "." );
	}

	private boolean isMapingTable(Table table) {
		int tableColumns = 0;
		for ( Iterator iterator = table.getColumnIterator(); iterator.hasNext(); ) {
			Object next = iterator.next();
			log.debug( "column: " + next );
			tableColumns++;
		}
		log.debug( "table columns: " + tableColumns );
		return table.getPrimaryKey() == null && tableColumns == 2;
	}

	private EmbedColumnName prepareColumnNames(Column column) {
		EmbedColumnName names = null;
		String columnName = column.getName();
		Matcher matcher = PATTERN.matcher( columnName );
		if ( matcher.find() ) {
			String mainFieldName = matcher.group( 1 );
			String emdeddedFieldName = matcher.group( 2 );
			names = new EmbedColumnName( mainFieldName, emdeddedFieldName );
		}
		return names;
	}

	private String searchMappedByName(SchemaDefinitionContext context, Collection<Table> tables, EntityType type, Column currentColumn) {
		String columnName = currentColumn.getName();
		String tableName = type.getAssociatedJoinable( context.getSessionFactory() ).getTableName();

		String primaryKeyName = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( tableName ) ) {
				primaryKeyName = table.getPrimaryKey().getColumn( 0 ).getName();
			}
		}
		return columnName.replace( "_" + primaryKeyName, "" );

	}

	private Class searchMappedByReturnedClass(SchemaDefinitionContext context, Collection<Table> tables, EntityType type, Column currentColumn) {
		String tableName = type.getAssociatedJoinable( context.getSessionFactory() ).getTableName();

		Class primaryKeyClass = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( tableName ) ) {
				log.debug( "primary key type: " + table.getPrimaryKey().getColumn( 0 ).getValue().getType().getReturnedClass() );
				primaryKeyClass = table.getPrimaryKey().getColumn( 0 ).getValue().getType().getReturnedClass();
			}
		}
		return primaryKeyClass;
	}

	@Override
	public void validateMapping(SchemaDefinitionContext context) {
		log.debug( "start" );
		super.validateMapping( context ); // To change body of generated methods, choose Tools | Templates.
	}

	public static String generateSeqName(String tableName, String primaryKeyName) {
		StringBuilder buffer = new StringBuilder();
		buffer.append( "seq_" ).append( tableName.toLowerCase() ).append( "_" ).append( primaryKeyName.toLowerCase() );
		return buffer.toString();
	}

}
