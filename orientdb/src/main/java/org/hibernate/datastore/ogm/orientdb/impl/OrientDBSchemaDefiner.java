/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.impl;

import com.orientechnologies.common.exception.OException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.CustomType;
import org.hibernate.type.EntityType;
import org.hibernate.type.EnumType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.ManyToOneType;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.StringType;
import org.hibernate.usertype.UserType;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.CharBuffer;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBMapping;
import org.hibernate.type.ComponentType;
import org.hibernate.type.descriptor.converter.AttributeConverterTypeAdapter;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBSchemaDefiner extends BaseSchemaDefiner {

	private static final String CREATE_PROPERTY_TEMPLATE = "create property {0}.{1} {2}";
	private static final String CREATE_EMBEDDED_PROPERTY_TEMPLATE = "create property {0}.{1} embedded {2}";
	private static final Log log = LoggerFactory.getLogger();
	private static final Pattern PATTERN = Pattern.compile( "directed([a-zA-Z_0-9])\\.(.+)" );
	private static final Set<Class> RELATIONS_TYPES;
	private static final Map<Class, Class> RETURNED_CLASS_TYPE_MAPPING;

	private static final Set<Class> SEQ_TYPES;

	private OrientDBDatastoreProvider provider;

	static {

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

	private String createClassQuery(String tableName) {
		return String.format( "create class %s extends V", tableName );
	}

	private String createClassQuery(Table table) {
		String query = null;
		if ( isTablePerClassInheritance( table ) ) {
			query = String.format( "create class %s extends %s", table.getName(), table.getPrimaryKey().getTable().getName() );

		}
		else {
			query = String.format( "create class %s extends V", table.getName() );
		}
		return query;
	}

	private void createSequence(Connection connection, String name, int startValue, int incValue) {
		try {
			String query = String.format( "CREATE SEQUENCE %s TYPE ORDERED START %d INCREMENT %d", name, ( startValue == 0 ? 0 : startValue - 1 ), incValue );
			log.debugf( "query for create sequnce: %s", query );
			connection.createStatement().execute( query );
		}
		catch (OSequenceException ose) {
			log.warnf( "OSequenceException: %s", ose.getMessage() );
		}
		catch (SQLException | OException e) {
			throw log.cannotGenerateSequence( name, e );
		}
	}

	private void createTableSequence(Connection connection, String seqTable, String pkColumnName, String valueColumnName) {
		try {
			connection.createStatement().execute( String.format( "create class %s extends V", seqTable ) );
			connection.createStatement().execute( String.format( "create property %s.%s string ", seqTable, pkColumnName ) );
			connection.createStatement().execute( String.format( "create property %s.%s long ", seqTable, valueColumnName ) );
			connection.createStatement().execute( String.format( "create index %s.%s unique ", seqTable, pkColumnName ) );
		}
		catch (SQLException | OException e) {
			throw log.cannotGenerateClass( seqTable, e );
		}
	}

	private void createGetTableSeqValueFunc(Connection connection) {
		try {
			OrientJdbcConnection orientDBConnection = (OrientJdbcConnection) connection;
			Set<String> functions = orientDBConnection.getDatabase().getMetadata().getFunctionLibrary().getFunctionNames();
			log.debugf( " functions : %s", functions );
			if ( functions.contains( "getTableSeqValue".toUpperCase() ) ) {
				log.debug( " function 'getTableSeqValue' exists!" );
				return;
			}
			InputStream is = OrientDBSchemaDefiner.class.getResourceAsStream( "getTableSeq.sql" );
			Reader reader = new InputStreamReader( is, "utf-8" );
			char[] chars = new char[2000];
			CharBuffer buffer = CharBuffer.wrap( chars );
			int realReadChars = reader.read( buffer );
			log.debugf( "getTableSeqValue query: %s ; realReadChars: %d", new String( buffer.array() ).trim(), realReadChars );
			connection.createStatement().execute( new String( buffer.array() ).trim() );
		}
		catch (SQLException | OException | IOException e) {
			throw log.cannotCreateStoredProcedure( "getTableSeqValue", e );
		}
	}

	private void createEntities(Connection connection, SchemaDefinitionContext context) {

		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			Set<String> createdEmbeddedClassSet = new HashSet<>();
			Set<String> tables = new HashSet<>();
			for ( Sequence sequence : namespace.getSequences() ) {
				log.debugf( "sequence.getName(): %s", sequence.getName() );
				createSequence( connection, sequence.getName().getSequenceName().getCanonicalName(),
						sequence.getInitialValue(), sequence.getIncrementSize() );
			}

			for ( Table table : namespace.getTables() ) {
				log.debugf( "table name: %s, abstract union table: %b; physical table: %b; abstract table: %b ",
						table.getName(), table.isAbstractUnionTable(), table.isPhysicalTable(), table.isAbstract() );
				log.debugf( "QualifiedTableName: ObjectName: %s; TableName:%s ",
						table.getQualifiedTableName().getObjectName(), table.getQualifiedTableName().getTableName() );

				boolean isMappingTable = isMapingTable( table );
				boolean isEmbeddedListTableName = isEmbeddedListTable( table );
				String tableName = table.getName();

				if ( isEmbeddedListTableName ) {
					tableName = table.getName().substring( 0, table.getName().indexOf( "_" ) );

					EmbeddedColumnInfo embeddedListColumn = new EmbeddedColumnInfo( table.getName().substring( table.getName().indexOf( "_" ) + 1 ) );
					log.debugf( "table %s is table for embedded collections! EmbeddedList class: %s; embedded list property: %s",
							table.getName(), embeddedListColumn.getClassNames(), embeddedListColumn.getPropertyName() );
					for ( String className : embeddedListColumn.getClassNames() ) {
						if ( !createdEmbeddedClassSet.contains( className ) ) {
							String classQuery = createClassQuery( className );
							log.debugf( "create class query: %s", classQuery );
							try {
								connection.createStatement().execute( classQuery );
								tables.add( className );
							}
							catch (SQLException e) {
								throw log.cannotGenerateClass( table.getName(), e );
							}
						}
					}
					String createEmbeddedListQuery = String.format( "create property %s.%s embeddedlist %s ",
							embeddedListColumn.getClassNames().get( embeddedListColumn.getClassNames().size() - 1 ),
							embeddedListColumn.getPropertyName(),
							embeddedListColumn.getPropertyName() );
					log.debugf( "create embeddedlist query: %s", createEmbeddedListQuery );
					throw new UnsupportedOperationException( String.format( "Table name %s not supported!", tableName ) );
				}
				else {
					String classQuery = createClassQuery( table );
					log.debugf( "create class query: %s", classQuery );
					try {
						provider.getConnection().createStatement().execute( classQuery );
						tables.add( tableName );
					}
					catch (SQLException | OException e) {
						log.error( "cannotGenerateClass!", e );
						throw log.cannotGenerateClass( table.getName(), e );
					}
				}

				Iterator<Column> columnIterator = table.getColumnIterator();

				while ( columnIterator.hasNext() ) {
					Column column = columnIterator.next();
					log.debugf( "column: %s ", column );
					log.debugf( "relation type: %s", column.getValue().getType().getClass() );

					if ( OrientDBConstant.SYSTEM_FIELDS.contains( column.getName() ) ) {
						continue;
					}
					else if ( ComponentType.class.equals( column.getValue().getType().getClass() ) ) {
						log.debugf( "column name %s has component type. Returned type: %s ",
								column.getName(), column.getValue().getType().getReturnedClass() );
						ComponentType type = (ComponentType) column.getValue().getType();
					}
					else if ( RELATIONS_TYPES.contains( column.getValue().getType().getClass() ) ) {
						// @TODO refactor it
						Value value = column.getValue();
						log.debugf( "column name: %s ; column.getCanonicalName(): %s", column.getName(), column.getCanonicalName() );
						if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
							EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
							log.debugf( "embedded column. class: %s ; property: %s", ec.getClassNames().get( 0 ), ec.getPropertyName() );

						}
						else {
							Class mappedByClass = searchMappedByReturnedClass( context, namespace.getTables(), (EntityType) value.getType(), column );
							String propertyQuery = createValueProperyQuery( table, column, RETURNED_CLASS_TYPE_MAPPING.get( mappedByClass ) );
							log.debugf( "create foreign key property query: %s", propertyQuery );
							try {
								provider.getConnection().createStatement().execute( propertyQuery );
							}
							catch (SQLException e) {
								throw log.cannotGenerateProperty( column.getName(), table.getName(), e );
							}
						}
						// @TODO use Links as foreign keys. see http://orientdb.com/docs/last/SQL-Create-Link.html

					}
					else if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
						EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
						log.debugf( "embedded column. class: %s ; property: %s", ec.getClassNames(), ec.getPropertyName() );
						createEmbeddedColumn( createdEmbeddedClassSet, tableName, column, ec );
					}
					else {
						String propertyQuery = createValueProperyQuery( tableName, column );
						log.debugf( "create property query: %s", propertyQuery );
						try {
							provider.getConnection().createStatement().execute( propertyQuery );
						}
						catch (OCommandExecutionException oe) {
							log.debugf( "orientdb message: %s; ", oe.getMessage() );
							if ( oe.getMessage().contains( "already exists" ) ) {
								log.debugf( "property %s already exists. Continue ", column.getName() );
							}
							else {
								throw log.cannotExecuteQuery( propertyQuery, oe );
							}
						}
						catch (SQLException | OException e) {
							log.error( "Exception:", e );
							throw log.cannotGenerateProperty( column.getName(), table.getName(), e );
						}

					}
				}
				// @TODO think about primary key for mapping tables!
				if ( table.hasPrimaryKey() && !isTablePerClassInheritance( table )
						&& !isEmbeddedObjectTable( table ) && !isMapingTable( table ) ) {
					PrimaryKey primaryKey = table.getPrimaryKey();
					if ( primaryKey != null ) {
						log.debugf( "primaryKey: %s ", primaryKey.getTable().getName() );
						createPrimaryKey( connection, primaryKey );
					}
					else {
						log.debugf( "Table %s has not primary key", table.getName() );
					}
				}
			}
		}

	}

	private boolean isTablePerClassInheritance(Table table) {
		if ( !table.hasPrimaryKey() ) {
			return false;
		}
		String primaryKeyTableName = table.getPrimaryKey().getTable().getName();
		String tableName = table.getName();
		return !tableName.equals( primaryKeyTableName );
	}

	private void createPrimaryKey(Connection connection, PrimaryKey primaryKey) {
		String table = primaryKey.getTable().getName();
		StringBuilder uniqueIndexQuery = new StringBuilder( 100 );
		uniqueIndexQuery.append( "CREATE INDEX " ).append( table ).append( "_" );
		String firstColumn = primaryKey.getColumn( 0 ).getName();
		uniqueIndexQuery.append( firstColumn ).append( "_pk ON " ).append( table ).append( " (" ).append( firstColumn ).append( ") UNIQUE" );

		try {
			log.debugf( "query: %s", uniqueIndexQuery );
			connection.createStatement().execute( uniqueIndexQuery.toString() );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( uniqueIndexQuery.toString(), e );
		}
		StringBuilder seq = new StringBuilder( 100 );
		if ( primaryKey.getColumns().size() == 1 && SEQ_TYPES.contains( primaryKey.getColumns().get( 0 ).getValue().getType().getClass() ) ) {
			seq.append( "CREATE SEQUENCE " );
			seq.append( generateSeqName( primaryKey.getTable().getName(), primaryKey.getColumns().get( 0 ).getName() ) );
			seq.append( " TYPE ORDERED START 0" );
			try {
				log.debugf( "query: %s", seq );
				connection.createStatement().execute( seq.toString() );
			}
			catch (SQLException e) {
				throw log.cannotExecuteQuery( seq.toString(), e );
			}
		}
	}

	private String createValueProperyQuery(Table table, Column column) {
		SimpleValue simpleValue = (SimpleValue) column.getValue();
		return createValueProperyQuery( table, column, simpleValue.getType().getClass() );
	}

	private String createValueProperyQuery(String tableName, Column column) {
		SimpleValue simpleValue = (SimpleValue) column.getValue();
		return createValueProperyQuery( tableName, column, simpleValue.getType().getClass() );
	}

	private void createEmbeddedColumn(Set<String> createdEmbeddedClassSet, String tableName, Column column, EmbeddedColumnInfo ec) {
		LinkedList<String> allClasses = new LinkedList<>();
		allClasses.add( tableName );
		allClasses.addAll( ec.getClassNames() );
		allClasses.add( ec.getPropertyName() );
		for ( int classIndex = 0; classIndex < allClasses.size() - 1; classIndex++ ) {
			String propertyOwnerClassName = allClasses.get( classIndex );
			log.debugf( "createdEmbeddedClassSet: %s; ", createdEmbeddedClassSet );
			if ( classIndex + 1 < allClasses.size() - 1 ) {
				String embeddedClassName = allClasses.get( classIndex + 1 );
				if ( !createdEmbeddedClassSet.contains( embeddedClassName ) ) {
					log.debugf( "11.propertyOwnerClassName: %s; propertyName: %s;embeddedClassName:%s; classIndex:%d",
							propertyOwnerClassName, embeddedClassName, embeddedClassName, classIndex );
					String executedQuery = null;
					try {
						executedQuery = createClassQuery( embeddedClassName );
						provider.getConnection().createStatement().execute( executedQuery );
						executedQuery = MessageFormat.format( CREATE_EMBEDDED_PROPERTY_TEMPLATE,
								propertyOwnerClassName, embeddedClassName, embeddedClassName );
						log.debugf( "1.query: %s; ", executedQuery );
						provider.getConnection().createStatement().execute( executedQuery );
						createdEmbeddedClassSet.add( embeddedClassName );
					}
					catch (SQLException sqle) {
						throw log.cannotExecuteQuery( executedQuery, sqle );
					}
				}
				else {
					log.debugf( "11.propertyOwnerClassName: %s and  propertyName: %s already created",
							propertyOwnerClassName, embeddedClassName );
				}
			}
			else {
				String valuePropertyName = allClasses.get( classIndex + 1 );
				log.debugf( "12.propertyOwnerClassName: %s; valuePropertyName: %s; classIndex:%d",
						propertyOwnerClassName, valuePropertyName, classIndex );
				SimpleValue simpleValue = (SimpleValue) column.getValue();
				String executedQuery = null;
				try {
					executedQuery = createValueProperyQuery( column, propertyOwnerClassName, valuePropertyName, simpleValue.getType().getClass() );
					log.debugf( "2.query: %s; ", executedQuery );
					provider.getConnection().createStatement().execute( executedQuery );
				}
				catch (SQLException sqle) {
					throw log.cannotExecuteQuery( executedQuery, sqle );
				}
				catch (OCommandExecutionException oe) {
					log.debugf( "orientdb message: %s; ", oe.getMessage() );
					if ( oe.getMessage().contains( ".".concat( valuePropertyName ) ) && oe.getMessage().contains( "already exists" ) ) {
						log.debugf( "property %s.%s already exists. Continue ", propertyOwnerClassName, valuePropertyName );
					}
					else {
						throw log.cannotExecuteQuery( executedQuery, oe );
					}

				}
			}
		}
	}

	private String createValueProperyQuery(Column column, String className, String propertyName, Class targetTypeClass) {

		String query = null;
		if ( targetTypeClass.equals( CustomType.class ) ) {
			CustomType type = (CustomType) column.getValue().getType();
			log.debug( "2.Column " + column.getName() + " :" + type.getUserType() );
			UserType userType = type.getUserType();
			if ( userType instanceof EnumType ) {
				EnumType enumType = (EnumType) type.getUserType();
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						className, propertyName, OrientDBMapping.TYPE_MAPPING.get( enumType.isOrdinal() ? IntegerType.class : StringType.class ) );

			}
			else {
				throw new UnsupportedOperationException( "Unsupported user type: " + userType.getClass() );
			}
		}
		else if ( targetTypeClass.equals( AttributeConverterTypeAdapter.class ) ) {
			log.debug( "3.Column  name: " + column.getName() + " ; className: " + column.getValue().getType().getClass() );
			AttributeConverterTypeAdapter type = (AttributeConverterTypeAdapter) column.getValue().getType();
			int sqlType = type.getSqlTypeDescriptor().getSqlType();
			log.debugf( "3.sql type: %d", sqlType );
			if ( !OrientDBMapping.SQL_TYPE_MAPPING.containsKey( sqlType ) ) {
				throw new UnsupportedOperationException( "Unsupported SQL type: " + sqlType );
			}
			query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
					className, propertyName, OrientDBMapping.SQL_TYPE_MAPPING.get( sqlType ) );
		}
		else {
			String orientDbTypeName = OrientDBMapping.TYPE_MAPPING.get( targetTypeClass );
			if ( orientDbTypeName == null ) {
				throw new UnsupportedOperationException( "Unsupported type: " + targetTypeClass );
			}
			else {
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						className, propertyName, orientDbTypeName );
			}

		}
		return query;
	}

	private String createValueProperyQuery(String tableName, Column column, Class targetTypeClass) {
		log.debugf( "1.Column: %s, targetTypeClass: %s ", column.getName(), targetTypeClass );
		return createValueProperyQuery( column, tableName, column.getName(), targetTypeClass );

	}

	private String createValueProperyQuery(Table table, Column column, Class targetTypeClass) {
		log.debugf( "1.Column: %s, targetTypeClass: %s ", column.getName(), targetTypeClass );
		return createValueProperyQuery( column, table.getName(), column.getName(), targetTypeClass );

	}

	private boolean isMapingTable(Table table) {
		Set<String> tableColumns = new HashSet<>();
		Set<String> primaryKeyColumns = new HashSet<>();
		for ( Iterator<Column> iterator = table.getColumnIterator(); iterator.hasNext(); ) {
			Column column = iterator.next();
			tableColumns.add( column.getName() );
		}
		log.debugf( "isMapingTable: Table: %s, primary key: %s ", table.getName(), table.getPrimaryKey() );
		if ( table.hasPrimaryKey() ) {
			for ( Column column : table.getPrimaryKey().getColumns() ) {
				primaryKeyColumns.add( column.getName() );
			}
		}
		// @TODO think about multucolumn index in OrientDB!
		return ( !table.hasPrimaryKey() && tableColumns.size() == 2 ) ||
				( tableColumns.equals( primaryKeyColumns ) );
	}

	private boolean isEmbeddedObjectTable(Table table) {
		return table.getName().contains( "_" );
	}

	private boolean isEmbeddedListTable(Table table) {
		int p1 = table.getName().indexOf( "_" );
		int p2 = table.getName().indexOf( ".", p1 );
		return p1 > -1 && p2 > p1;
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
	public void initializeSchema(SchemaDefinitionContext context) {
		log.debug( "initializeSchema" );
		SessionFactoryImplementor sessionFactoryImplementor = context.getSessionFactory();
		ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();
		provider = (OrientDBDatastoreProvider) registry.getService( DatastoreProvider.class );
		Connection connection = provider.getConnection();
		createSequence( connection, OrientDBConstant.HIBERNATE_SEQUENCE, 0, 1 );
		createTableSequence( connection, OrientDBConstant.HIBERNATE_SEQUENCE_TABLE, "key", "seed" );
		createGetTableSeqValueFunc( connection );
		createEntities( connection, context );
	}

	@Override
	public void validateMapping(SchemaDefinitionContext context) {
		log.debug( "validateMapping" );
		super.validateMapping( context );
	}

	public static String generateSeqName(String tableName, String primaryKeyName) {
		StringBuilder buffer = new StringBuilder();
		buffer.append( "seq_" ).append( tableName.toLowerCase() ).append( "_" ).append( primaryKeyName.toLowerCase() );
		return buffer.toString();
	}

}
