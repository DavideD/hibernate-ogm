/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.binarystorage;

import org.bson.Document;
import org.hibernate.ogm.options.spi.OptionsContext;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public interface BinaryStorage {

	void storeContentToBinaryStorage(OptionsContext optionsContext, Document currentDocument, String fieldName);

	void removeContentFromBinaryStore(OptionsContext optionsContext, Document deletedDocument, String fieldName);

	void loadContentFromBinaryStorageToField(OptionsContext optionsContext, Document currentDocument, String fieldName);
}
