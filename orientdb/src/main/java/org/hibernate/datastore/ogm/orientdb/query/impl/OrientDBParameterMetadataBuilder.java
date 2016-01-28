/*
 * Copyright (C) 2015 Hibernate.
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

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.engine.query.spi.ParameterParser;
import org.hibernate.ogm.dialect.query.spi.RecognizerBasedParameterMetadataBuilder;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.RecoveringParseRunner;

/**
 *
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBParameterMetadataBuilder extends RecognizerBasedParameterMetadataBuilder {

    private static Log LOG = LoggerFactory.getLogger();

    @Override
    public void parseQueryParameters(String nativeQuery, ParameterParser.Recognizer journaler) {
        OrientDBQueryParser parser = Parboiled.createParser(OrientDBQueryParser.class, journaler);
        new RecoveringParseRunner<ParameterParser.Recognizer>(parser.Query()).run(nativeQuery);
    }

}
