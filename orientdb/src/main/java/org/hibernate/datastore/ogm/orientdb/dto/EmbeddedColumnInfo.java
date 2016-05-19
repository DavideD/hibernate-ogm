/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dto;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class EmbeddedColumnInfo {

	private LinkedList<String> classNames;
	private String propertyName;
	private String classNamesAdd;

	public EmbeddedColumnInfo(String fullPropertyName, String classNamesAdd) {
		this( fullPropertyName );
		if ( classNamesAdd != null && classNamesAdd.trim().length() > 0 ) {
			classNames.addFirst( classNamesAdd );
		}
	}

	public EmbeddedColumnInfo(String fullPropertyName) {
		String[] parts = fullPropertyName.split( "\\." );
		classNames = new LinkedList<>();
		for ( String part : parts ) {
			classNames.add( part );
		}
		propertyName = classNames.getLast();
		classNames.removeLast();
	}

	public List<String> getClassNames() {
		return classNames;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getClassNamesAdd() {
		return classNamesAdd;
	}

	@Override
	public String toString() {
		return "EmbeddedColumnInfo{" + "classNames=" + classNames + ", propertyName=" + propertyName + ", classNamesAdd=" + classNamesAdd + '}';
	}
}
