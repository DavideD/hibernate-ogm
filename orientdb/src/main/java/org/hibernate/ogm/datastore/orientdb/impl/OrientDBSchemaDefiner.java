/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.ogm.datastore.orientdb.impl;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.ogm.datastore.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.datastore.orientdb.utils.NativeQueryUtil;
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
import org.hibernate.type.StringType;
import org.hibernate.usertype.UserType;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.CreateParams;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;

import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBMapping;
import org.hibernate.type.ComponentType;
import org.hibernate.type.descriptor.converter.AttributeConverterTypeAdapter;

/**
 * Schema definer for OrientDB
 * <p>
 * Implementation details:
 * </p>
 * <ol>
 * <li>Annotation "EmbeddedId" is not supported</li>
 * <li>Annotation "CompositeId" is supported partly</li>
 * <li>Primary key created as unique index</li>
 * <li>Associations between entities is like relational DBMS (by link owner field)</li>
 * </ol>
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBSchemaDefiner extends BaseSchemaDefiner {

	private static final long serialVersionUID = 1L;
	private static final String CREATE_PROPERTY_TEMPLATE = "create property {0}.{1} {2}";
	private static final String CREATE_EMBEDDED_PROPERTY_TEMPLATE = "create property {0}.{1} embedded {2}";
	private static final Log log = LoggerFactory.getLogger();

	private OrientDBDatastoreProvider provider;

	private String createClassQuery(String tableName) {
		return String.format( "create class %s extends V", tableName );
	}

	private String createClassQuery(Table table) {
		if ( isTablePerClassInheritance( table ) ) {
			return String.format( "create class %s extends %s", table.getName(), table.getPrimaryKey().getTable().getName() );
		}
		else {
			return String.format( "create class %s extends V", table.getName() );
		}
	}

	private void createSequence(ODatabaseDocumentTx db, String name, int startValue, int incValue) {
		CreateParams p = new CreateParams();
		p.setStart( (long) ( startValue == 0 ? 0 : startValue - 1 ) );
		p.setIncrement( incValue );
		OSequence seq = db.getMetadata().getSequenceLibrary().createSequence( name, SEQUENCE_TYPE.ORDERED, p );
		log.debugf( " sequence  %s created ", seq.getName() );
	}

	private void createTableSequence(ODatabaseDocumentTx db, String seqTable, String pkColumnName, String valueColumnName) {

		NativeQueryUtil.executeNonIdempotentQuery( db, String.format( "create class %s extends V", seqTable ) );
		NativeQueryUtil.executeNonIdempotentQuery( db, String.format( "create property %s.%s string ", seqTable, pkColumnName ) );
		NativeQueryUtil.executeNonIdempotentQuery( db, String.format( "create property %s.%s long ", seqTable, valueColumnName ) );
		NativeQueryUtil.executeNonIdempotentQuery( db, String.format( "create index %s.%s unique ", seqTable, pkColumnName ) );

	}

	private void createGetTableSeqValueFunc(ODatabaseDocumentTx db) {
		try {

			Set<String> functions = db.getMetadata().getFunctionLibrary().getFunctionNames();
			log.debugf( " functions : %s", functions );
			if ( functions.contains( "getTableSeqValue".toUpperCase() ) ) {
				log.debug( " function 'getTableSeqValue' exists!" );
				return;
			}
			for ( OClass oc : db.getMetadata().getSchema().getClasses() ) {
				log.debugf( " class: %s", oc.getName() );
			}

			OFunction executeQuery = db.getMetadata().getFunctionLibrary().createFunction( "getTableSeqValue" );
			executeQuery.setLanguage( "groovy" );
			executeQuery.setIdempotent( false );
			executeQuery.setParameters( Arrays.asList( new String[]{ "seqName", "pkColumnName",
					"pkColumnValue", "valueColumnName", "initValue", "inc" } ) );
			executeQuery.setCode( "long nextValue=-1;def db = orient.getDatabase();"
					+ " def results = null; "
					+ "String updateQuery=\"UPDATE ${seqName} INCREMENT ${valueColumnName} = ${inc} return after \\$current WHERE ${pkColumnName} = '${pkColumnValue}'\";"
					+ " String selectQuery = \"select from ${seqName} where ${pkColumnName}='{pkColumnValue}'\";"
					+ " String insertQuery = \"insert into ${seqName} (${pkColumnName},${valueColumnName}) values('${pkColumnValue}',${initValue})\";"
					+ " try {results = db.command(updateQuery); if (results.length==0)"
					+ " {try {db.command(insertQuery);} catch (Exception e2) {throw e2;}; nextValue=Long.parseLong(\"${initValue}\"); } "
					+ "else { nextValue=results[0].field(\"${valueColumnName}\");}; } catch (Exception e) { throw e; };   "
					+ "return nextValue; " );
			executeQuery.save();
		}
		catch (RuntimeException e) {
			throw log.cannotCreateStoredProcedure( "getTableSeqValue", e );
		}
	}

	private void createExecuteQueryFunc(ODatabaseDocumentTx db) {
		try {

			Set<String> functions = db.getMetadata().getFunctionLibrary().getFunctionNames();
			log.debugf( " functions : %s", functions );
			if ( functions.contains( "executeQuery".toUpperCase() ) ) {
				log.debug( " function 'executeQuery' exists!" );
				return;
			}
			for ( OClass oc : db.getMetadata().getSchema().getClasses() ) {
				log.debugf( " class: %s", oc.getName() );
			}

			OFunction executeQuery = db.getMetadata().getFunctionLibrary().createFunction( "executeQuery" );
			executeQuery.setLanguage( "groovy" );
			executeQuery.setIdempotent( false );
			executeQuery.setParameters( Arrays.asList( new String[]{ "insertQuery" } ) );
			executeQuery.setCode( "return orient.getDatabase().command(insertQuery);" );
			executeQuery.save();

		}
		catch (RuntimeException e) {
			throw log.cannotCreateStoredProcedure( "executeQuery", e );
		}
	}

	private boolean isAlreadyCreatedInParent(Table currentTable, Column currentColumn, Collection<Table> tables) {
		boolean created = false;
		Table inheritedFromTable = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( currentTable.getPrimaryKey().getTable().getName() ) ) {
				inheritedFromTable = table;
				break;
			}
		}
		if ( inheritedFromTable != null ) {
			for ( @SuppressWarnings("unchecked")
			Iterator<Column> it = inheritedFromTable.getColumnIterator(); it.hasNext(); ) {
				Column column = it.next();
				if ( currentColumn.getName().equals( column.getName() ) ) {
					created = true;
					break;
				}
			}
		}
		return created;
	}

	private void createEntities(ODatabaseDocumentTx db, SchemaDefinitionContext context) {

		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Sequence sequence : namespace.getSequences() ) {
				createSequence( db, sequence.getName().getSequenceName().getCanonicalName(), sequence.getInitialValue(), sequence.getIncrementSize() );
			}

			Set<String> tables = new HashSet<>();
			Set<String> createdEmbeddedClassSet = new HashSet<>();
			for ( Table table : namespace.getTables() ) {
				boolean isEmbeddedListTableName = isEmbeddedListTable( table );
				String tableName = table.getName();

				if ( isEmbeddedListTableName ) {
					tableName = table.getName().substring( 0, table.getName().indexOf( "_" ) );

					EmbeddedColumnInfo embeddedListColumn = new EmbeddedColumnInfo( table.getName().substring( table.getName().indexOf( "_" ) + 1 ) );
					for ( String className : embeddedListColumn.getClassNames() ) {
						if ( !createdEmbeddedClassSet.contains( className ) ) {
							String classQuery = createClassQuery( className );
							// try {
							NativeQueryUtil.executeNonIdempotentQuery( db, classQuery );
							tables.add( className );
							/*
							 * } catch (SQLException e) { throw log.cannotGenerateClass( table.getName(), e ); }
							 */
						}
					}

					throw new UnsupportedOperationException( String.format( "Table name %s not supported!", tableName ) );
				}
				else {
					String classQuery = createClassQuery( table );
					// try {
					NativeQueryUtil.executeNonIdempotentQuery( db, classQuery );
					tables.add( tableName );
					/*
					 * } catch (SQLException e) { throw log.cannotGenerateClass( table.getName(), e ); }
					 */
				}

				@SuppressWarnings("unchecked")
				Iterator<Column> columnIterator = table.getColumnIterator();
				while ( columnIterator.hasNext() ) {
					Column column = columnIterator.next();
					log.debugf( "column: %s ", column );
					log.debugf( "relation type: %s", column.getValue().getType().getClass() );

					if ( column.getName().startsWith( "_identifierMapper" ) ||
							OrientDBConstant.SYSTEM_FIELDS.contains( column.getName() ) ||
							( isTablePerClassInheritance( table ) && isAlreadyCreatedInParent( table, column, namespace.getTables() ) ) ) {
						continue;
					}

					if ( ComponentType.class.equals( column.getValue().getType().getClass() ) ) {
						throw new UnsupportedOperationException( "Component type not supported yet " );
					}
					else if ( OrientDBMapping.RELATIONS_TYPES.contains( column.getValue().getType().getClass() ) ) {
						Value value = column.getValue();
						if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
							EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
							// TODO: ???
						}
						else {
							Class<?> mappedByClass = searchMappedByReturnedClass( context, namespace.getTables(), (EntityType) value.getType(), column );
							String propertyQuery = createValueProperyQuery( table, column, OrientDBMapping.FOREIGN_KEY_TYPE_MAPPING.get( mappedByClass ) );
							// try {
							NativeQueryUtil.executeNonIdempotentQuery( db, propertyQuery );
							/*
							 * } catch (SQLException e) { throw log.cannotGenerateProperty( column.getName(),
							 * table.getName(), e ); }
							 */
						}
					}
					else if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
						EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
						boolean isPrimaryKeyColumn = isPrimaryKeyColumn( table, column );
						if ( !isPrimaryKeyColumn ) {
							createEmbeddedColumn( createdEmbeddedClassSet, tableName, column, ec );
						}
						else {
							String columnName = column.getName().substring( column.getName().indexOf( "." ) + 1 );
							SimpleValue simpleValue = (SimpleValue) column.getValue();
							String propertyQuery = createValueProperyQuery( column, tableName, columnName,
									simpleValue.getType().getClass() );
							log.debugf( "create property query: %s", propertyQuery );
							// try {
							NativeQueryUtil.executeNonIdempotentQuery( db, propertyQuery );
							/*
							 * } catch (SQLException e) { log.error( "Exception:", e ); throw
							 * log.cannotGenerateProperty( column.getName(), table.getName(), e ); }
							 */
						}
					}
					else {
						String propertyQuery = createValueProperyQuery( tableName, column );
						try {
							NativeQueryUtil.executeNonIdempotentQuery( db, propertyQuery );
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
						/*
						 * catch (SQLException e) { log.error( "Exception:", e ); throw log.cannotGenerateProperty(
						 * column.getName(), table.getName(), e ); }
						 */

					}
				}

				if ( table.hasPrimaryKey() && !isTablePerClassInheritance( table ) && !isEmbeddedObjectTable( table ) ) {
					PrimaryKey primaryKey = table.getPrimaryKey();
					if ( primaryKey != null ) {
						createPrimaryKey( db, primaryKey );
					}
					else {
						log.debugf( "Table %s has not a primary key", table.getName() );
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

	private void createPrimaryKey(ODatabaseDocumentTx db, PrimaryKey primaryKey) {
		StringBuilder uniqueIndexQuery = new StringBuilder( 100 );
		uniqueIndexQuery.append( "CREATE INDEX " )
		.append( primaryKey.getName() != null
		? primaryKey.getName()
				: PrimaryKey.generateName( primaryKey.generatedConstraintNamePrefix(), primaryKey.getTable(), primaryKey.getColumns() ) )
		.append( " ON " ).append( primaryKey.getTable().getName() ).append( " (" );
		for ( Iterator<Column> it = primaryKey.getColumns().iterator(); it.hasNext(); ) {
			Column column = it.next();
			String columnName = column.getName();
			if ( columnName.contains( "." ) ) {
				// it is like embedded column .... but it is column for IdClass
				columnName = column.getName().substring( column.getName().indexOf( "." ) + 1 );
			}
			uniqueIndexQuery.append( columnName );
			if ( it.hasNext() ) {
				uniqueIndexQuery.append( "," );
			}
		}
		uniqueIndexQuery.append( ") UNIQUE" );

		// try {
		log.debugf( "primary key query: %s", uniqueIndexQuery );
		NativeQueryUtil.executeNonIdempotentQuery( db, uniqueIndexQuery );
		// connection.createStatement().execute( uniqueIndexQuery.toString() );
		/*
		 * } catch (SQLException e) { throw log.cannotExecuteQuery( uniqueIndexQuery.toString(), e ); }
		 */
		StringBuilder seq = new StringBuilder( 100 );
		if ( primaryKey.getColumns().size() == 1 && OrientDBMapping.SEQ_TYPES.contains( primaryKey.getColumns().get( 0 ).getValue().getType().getClass() ) ) {
			seq.append( "CREATE SEQUENCE " );
			seq.append( generateSeqName( primaryKey.getTable().getName(), primaryKey.getColumns().get( 0 ).getName() ) );
			seq.append( " TYPE ORDERED START 0" );
			// try {
			log.debugf( "query: %s", seq );
			// connection.createStatement().execute( seq.toString() );
			NativeQueryUtil.executeNonIdempotentQuery( db, seq );
			/*
			 * } catch (SQLException e) { throw log.cannotExecuteQuery( seq.toString(), e ); }
			 */
		}
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
					// try {
					executedQuery = createClassQuery( embeddedClassName );
					NativeQueryUtil.executeNonIdempotentQuery( provider.getCurrentDatabase(), executedQuery );
					executedQuery = MessageFormat.format( CREATE_EMBEDDED_PROPERTY_TEMPLATE,
							propertyOwnerClassName, embeddedClassName, embeddedClassName );
					log.debugf( "1.query: %s; ", executedQuery );
					NativeQueryUtil.executeNonIdempotentQuery( provider.getCurrentDatabase(), executedQuery );
					createdEmbeddedClassSet.add( embeddedClassName );
					/*
					 * } catch (SQLException sqle) { throw log.cannotExecuteQuery( executedQuery, sqle ); }
					 */
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
					NativeQueryUtil.executeNonIdempotentQuery( provider.getCurrentDatabase(), executedQuery );
				}
				/*
				 * catch (SQLException sqle) { throw log.cannotExecuteQuery( executedQuery, sqle ); }
				 */
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

	private String createValueProperyQuery(Column column, String className, String propertyName, Class<?> targetTypeClass) {
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
			AttributeConverterTypeAdapter<?> type = (AttributeConverterTypeAdapter<?>) column.getValue().getType();
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

	private String createValueProperyQuery(String tableName, Column column, Class<?> targetTypeClass) {
		log.debugf( "1.Column: %s, targetTypeClass: %s ", column.getName(), targetTypeClass );
		return createValueProperyQuery( column, tableName, column.getName(), targetTypeClass );

	}

	private String createValueProperyQuery(Table table, Column column, Class<?> targetTypeClass) {
		log.debugf( "1.Column: %s, targetTypeClass: %s ", column.getName(), targetTypeClass );
		return createValueProperyQuery( column, table.getName(), column.getName(), targetTypeClass );

	}

	private boolean isPrimaryKeyColumn(Table table, Column column) {
		boolean result = false;

		if ( table.hasPrimaryKey() ) {
			PrimaryKey primaryKey = table.getPrimaryKey();
			log.debugf( "isPrimaryKeyColumn:  primary key name: %s ", primaryKey.getName() );
			result = primaryKey.containsColumn( column );
		}

		return result;
	}

	private boolean isEmbeddedObjectTable(Table table) {
		return table.getName().contains( "_" );
	}

	private boolean isEmbeddedListTable(Table table) {
		int p1 = table.getName().indexOf( "_" );
		int p2 = table.getName().indexOf( ".", p1 );
		return p1 > -1 && p2 > p1;
	}

	private Class<?> searchMappedByReturnedClass(SchemaDefinitionContext context, Collection<Table> tables, EntityType type, Column currentColumn) {
		String tableName = type.getAssociatedJoinable( context.getSessionFactory() ).getTableName();
		log.debugf( "associated entity name: %s", type.getAssociatedEntityName() );

		Class<?> primaryKeyClass = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( tableName ) ) {
				log.debugf( "primary key type: %s", table.getPrimaryKey().getColumn( 0 ).getValue().getType().getReturnedClass() );
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
		ODatabaseDocumentTx db = provider.getCurrentDatabase();
		createExecuteQueryFunc( db );
		// createSequence( connection, OrientDBConstant.HIBERNATE_SEQUENCE, 0, 1 );
		createTableSequence( db, OrientDBConstant.HIBERNATE_SEQUENCE_TABLE, "key", "seed" );
		createGetTableSeqValueFunc( db );
		createEntities( db, context );
	}

	@Override
	public void validateMapping(SchemaDefinitionContext context) {
		log.debug( "validateMapping" );
		super.validateMapping( context );
	}

	/**
	 * generate name for sequence
	 *
	 * @param className name of OrientDB class
	 * @param primaryKeyName name of primary key
	 * @return name of sequence
	 */
	public static String generateSeqName(String className, String primaryKeyName) {
		StringBuilder buffer = new StringBuilder( 50 );
		buffer.append( "seq_" ).append( className.toLowerCase() ).append( "_" ).append( primaryKeyName.toLowerCase() );
		return buffer.toString();
	}
}
