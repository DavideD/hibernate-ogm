/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.text.DateFormat;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class FormatterUtil {

	private static ThreadLocal<DateFormat> dateFormater = null;
	private static ThreadLocal<DateFormat> dateTimeFormater = null;

	public static ThreadLocal<DateFormat> getDateFormater() {
		return dateFormater;
	}

	public static void setDateFormater(ThreadLocal<DateFormat> dateFormater) {
		FormatterUtil.dateFormater = dateFormater;
	}

	public static ThreadLocal<DateFormat> getDateTimeFormater() {
		return dateTimeFormater;
	}

	public static void setDateTimeFormater(ThreadLocal<DateFormat> dateTimeFormater) {
		FormatterUtil.dateTimeFormater = dateTimeFormater;
	}

}
