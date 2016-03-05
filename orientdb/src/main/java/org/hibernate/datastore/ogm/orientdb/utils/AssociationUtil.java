/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
<<<<<<< HEAD
 * 
=======
 *
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.id.ORecordId;
import java.sql.Connection;
import java.sql.SQLException;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class AssociationUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static String getMappedByFieldName(AssociationContext associationContext) {
		return associationContext.getAssociationTypeContext().getRoleOnMainSide();
	}

	public static String getMappedByFieldName(AssociatedEntityKeyMetadata associationAssociatedEntityKeyMetadata) {
		String associationKeyColumn = associationAssociatedEntityKeyMetadata.getAssociationKeyColumns()[0];
		String inversePrimaryKey = associationAssociatedEntityKeyMetadata.getEntityKeyMetadata().getColumnNames()[0];
		return associationKeyColumn.replace( "_".concat( inversePrimaryKey ), "" );
	}

<<<<<<< HEAD
=======
	@Deprecated
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	public static int removeAssociation(Connection connection, String edgeClassName, ORecordId outRid,
			ORecordId inRid) throws SQLException {
		StringBuilder query = new StringBuilder( 100 );
		query.append( "delete edge " ).append( edgeClassName ).append( " where out=" ).append( outRid );
		query.append( " and in=" ).append( inRid );
<<<<<<< HEAD
		log.info( "removeAssociations: query :" + query.toString() );
		return connection.createStatement().executeUpdate( query.toString() );
	}

=======
		log.debug( "removeAssociations: query :" + query.toString() );
		return connection.createStatement().executeUpdate( query.toString() );
	}

	@Deprecated
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	public static int insertAssociation(Connection connection, String edgeClassName, ORecordId outRid,
			ORecordId inRid) throws SQLException {
		StringBuilder query = new StringBuilder( 100 );
		query.append( "create edge " ).append( edgeClassName ).append( " from " ).append( outRid );
		query.append( " to " ).append( inRid );
<<<<<<< HEAD
		log.info( "insertAssociation: query :" + query.toString() );
=======
		log.debug( "insertAssociation: query :" + query.toString() );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		return connection.createStatement().executeUpdate( query.toString() );

	}

}
