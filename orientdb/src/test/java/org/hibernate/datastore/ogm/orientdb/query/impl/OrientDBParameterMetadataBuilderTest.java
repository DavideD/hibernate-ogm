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
package org.hibernate.datastore.ogm.orientdb.query.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.hibernate.engine.query.spi.NamedParameterDescriptor;
import org.hibernate.engine.query.spi.OrdinalParameterDescriptor;
import org.hibernate.engine.query.spi.ParamLocationRecognizer;
import org.hibernate.engine.query.spi.ParameterParser;
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
public class OrientDBParameterMetadataBuilderTest {

    public OrientDBParameterMetadataBuilderTest() {
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
     * Test of parseQueryParameters method, of class
     * OrientDBParameterMetadataBuilder.
     */
    @Test
    public void testParseQueryParameters() {
        System.out.println("parseQueryParameters");
        ParamLocationRecognizer recognizer = new ParamLocationRecognizer();
        String nativeQuery = "select from Customer where name=:name";
        OrientDBParameterMetadataBuilder instance = new OrientDBParameterMetadataBuilder();
        instance.parseQueryParameters(nativeQuery, recognizer);
        Set<String> parameters = instance.buildParameterMetadata(nativeQuery).getNamedParameterNames();
        System.out.println("1.parameters:"+parameters);
        assertNotNull("Parameters in null!!!!",parameters);
        assertFalse("Parameters in query must be!",parameters.isEmpty());
        System.out.println("1.parameters:"+parameters.contains("name"));
        assertTrue("Parameter 'name' must be!",parameters.contains("name"));
        
        nativeQuery = "select from #29:0";
        instance.parseQueryParameters(nativeQuery, recognizer);
        parameters = instance.buildParameterMetadata(nativeQuery).getNamedParameterNames();
        System.out.println("2.parameters:"+parameters);
        assertTrue("Parameters in query must not be!",parameters.isEmpty());
        
        
        nativeQuery = "select from Customer where name=:name2";
        instance.parseQueryParameters(nativeQuery, recognizer);
        parameters = instance.buildParameterMetadata(nativeQuery).getNamedParameterNames();
        System.out.println("3.parameters:"+parameters);
        assertFalse("Parameters in query must not be!",parameters.isEmpty());
        assertTrue("Parameter 'name2' must be!",parameters.contains("name2"));
        
    }

}
