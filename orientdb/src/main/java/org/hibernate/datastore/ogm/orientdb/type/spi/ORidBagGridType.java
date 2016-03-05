<<<<<<< HEAD
 /*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 * 
=======
/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.type.spi;

<<<<<<< HEAD
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.MappingException;
import org.hibernate.datastore.ogm.orientdb.type.descriptor.java.ORecordIdTypeDescriptor;
=======
import org.hibernate.MappingException;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
import org.hibernate.datastore.ogm.orientdb.type.descriptor.java.ORidBagTypeDescriptor;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.descriptor.impl.PassThroughGridTypeDescriptor;
import org.hibernate.ogm.type.impl.AbstractGenericBasicType;

<<<<<<< HEAD
/**
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */


public class ORidBagGridType extends AbstractGenericBasicType<ORidBag> {
    public static final ORidBagGridType INSTANCE = new ORidBagGridType();

    public ORidBagGridType() {
        super(PassThroughGridTypeDescriptor.INSTANCE, ORidBagTypeDescriptor.INSTANCE);
    }
    
    @Override
    public int getColumnSpan(Mapping mapping) throws MappingException {
        return 1;
    }

    @Override
    public String getName() {
        return "ORidBag";
    }
    
=======
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class ORidBagGridType extends AbstractGenericBasicType<ORidBag> {

	public static final ORidBagGridType INSTANCE = new ORidBagGridType();

	public ORidBagGridType() {
		super( PassThroughGridTypeDescriptor.INSTANCE, ORidBagTypeDescriptor.INSTANCE );
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}

	@Override
	public String getName() {
		return "ORidBag";
	}
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
}
