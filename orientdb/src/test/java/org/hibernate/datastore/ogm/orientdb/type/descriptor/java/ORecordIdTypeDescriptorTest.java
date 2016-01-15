/*
 * Copyright (C) 2016 Hibernate.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.hibernate.datastore.ogm.orientdb.type.descriptor.java;

import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.type.descriptor.WrapperOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author chernolyassv
 */
public class ORecordIdTypeDescriptorTest {
    
    public ORecordIdTypeDescriptorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of toString method, of class ORecordIdTypeDescriptor.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        ORecordId id = new ORecordId(30, 100L);
        ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
        String expResult = "#30:100";
        String result = instance.toString(id);
        assertEquals(expResult, result);        
    }

    /**
     * Test of fromString method, of class ORecordIdTypeDescriptor.
     */
    @Test
    public void testFromString() {
        System.out.println("fromString");
        String string = "#30:100";
        ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
        ORecordId expResult = new ORecordId(30, 100L);
        ORecordId result = instance.fromString(string);
        assertEquals(expResult, result);        
    }

    /**
     * Test of unwrap method, of class ORecordIdTypeDescriptor.
     */
    //@Test
    public void testUnwrap() {
        System.out.println("unwrap");
        ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
        Object expResult = null;
        //Object result = instance.unwrap(null);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of wrap method, of class ORecordIdTypeDescriptor.
     */
    //@Test
    public void testWrap() {
        System.out.println("wrap");
        Object x = null;
        WrapperOptions wo = null;
        ORecordIdTypeDescriptor instance = new ORecordIdTypeDescriptor();
        ORecordId expResult = null;
        ORecordId result = instance.wrap(x, wo);
        //assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }
    
}
