
package org.hibernate.datastore.ogm.orientdb.utils;

import java.text.DateFormat;

/**
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
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
