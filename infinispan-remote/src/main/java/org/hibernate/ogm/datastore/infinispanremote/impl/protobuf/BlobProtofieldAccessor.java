/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.infinispanremote.impl.protobuf;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import org.infinispan.protostream.MessageMarshaller.ProtoStreamReader;
import org.infinispan.protostream.MessageMarshaller.ProtoStreamWriter;

public class BlobProtofieldAccessor extends BaseProtofieldAccessor<Blob> implements ProtofieldAccessor<Blob> {

	public BlobProtofieldAccessor(int tag, String name, boolean nullable, String columnName) {
		super( tag, name, nullable, columnName,
				(ProtoStreamWriter outProtobuf, Blob value) -> outProtobuf.writeBytes( name, toBytes(value) ),
				(ProtoStreamReader reader) -> toBlob( reader.readBytes( name ) )
				);
	}

	@Override
	protected String getProtobufTypeName() {
		return "bytes";
	}

	private static byte[] toBytes(Blob value) {
		if ( value == null ) {
			try ( DataInputStream stream = new DataInputStream( value.getBinaryStream() ) ) {
				byte[] bytes = new byte[Long.valueOf( value.length() ).intValue()];
				stream.readFully( bytes );
				return bytes;
			}
			catch (IOException | SQLException e) {
				throw new RuntimeException( e );
			}
		}
		return null;
	}

	private static Blob toBlob(byte[] bytes) {
		if ( bytes != null ) {
			try {
				return new SerialBlob( bytes );
			}
			catch (SQLException e) {
				throw new RuntimeException( e );
			}
		}
		return null;
	}
}
