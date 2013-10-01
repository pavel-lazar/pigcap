
package pigcap.packet;

import pigcap.util.Buffer;


/**
 * PcapPacket contains header and payload.
 * 
 * @author pavel
 * 
 */
public class PcapPacket {
	private PacketHeader header;
	private Buffer payload;

	public PcapPacket(PacketHeader header, Buffer payload) {
		this.header = header;
		this.payload = payload;
	}

	public PacketHeader getPacketHeader() {
		return header;
	}

	public Buffer getPacketData() {
		return payload;
	}

	@Override
	public String toString() {
		return header.toString();
	}

}