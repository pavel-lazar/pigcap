package pigcap.packet;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigcap.util.Buffer;


public class PacketRecord {
	protected final int FRAME_TUPLE_SIZE = 3; // (timestamp, source, length)
	protected final int ETHERNET_TUPLE_SIZE = 4; // (source, destination, ethertype, data)
	protected final int IPV4_TUPLE_SIZE = 11; // (ihl, tos, length, id, flags, offset, ttl, protocol, source, destination, data)
	protected final int ICMPV4_TUPLE_SIZE = 4; // (type, code, other, data)
	protected final int UDP_TUPLE_SIZE = 4; // (source_port, dest_port, totalLength, data)
	protected final int TCP_TUPLE_SIZE = 9; // (source_port, dest_port, seq, ack, headerLength, flags, window, urgent, data)
	protected final int PACKET_TUPLE_SIZE = 6; // (frame, ethernet, ip, icmp, udp, tcp)
	protected TupleFactory tupleFactory = TupleFactory.getInstance();
	private PacketHeader packetHeader = null;
	private Buffer packetData = null;
	private int ethertype;
	private int ipProtocol; 
	public PacketRecord(PcapPacket pcapPacket) {		
		packetHeader = pcapPacket.getPacketHeader();
		packetData = pcapPacket.getPacketData();

	}
	
	public Tuple parseTupleFromPacket() {
		try {
			if ((packetHeader == null) || (packetData == null)) {
				return null;
			}
			
			Tuple frameTuple = parseFrameTuple();
			Tuple ethernetTuple = parseEthernetTuple();
			Tuple ipv4Tuple = null;
			Tuple icmpTuple = null;
			Tuple udpTuple = null;
			Tuple tcpTuple = null;
			
			if (ethernetTuple != null) {
				switch (ethertype) {
				case EthernetType.IPV4:
					ipv4Tuple = parseIpv4Tuple();
					break;
				default:
					DataByteArray data = payloadToDataByteArray();
					ethernetTuple.set(3, data);
					break;
				}
			}
			
			if (ipv4Tuple != null) {
				switch (ipProtocol) {
				case InternetProtocol.ICMP:
					icmpTuple = parseIcmpTuple();
					break;
				case InternetProtocol.UDP:
					udpTuple = parseUdpTuple();
					break;
				case InternetProtocol.TCP:
					tcpTuple = parseTcpTuple();
					break;
				default:
					ipv4Tuple.set(10, payloadToDataByteArray());
					break;
				}
			}
			
			// (frame, ethernet, ipv4, icmpv4, udp, tcp)
			Tuple packetTuple = tupleFactory.newTuple(PACKET_TUPLE_SIZE);
			packetTuple.set(0, frameTuple);
			packetTuple.set(1, ethernetTuple);
			packetTuple.set(2, ipv4Tuple);
			packetTuple.set(3, icmpTuple);
			packetTuple.set(4, udpTuple);
			packetTuple.set(5, tcpTuple);
			
			return packetTuple;
		} catch (ExecException e) {
			return null;
		}

	}

		
	protected Tuple parseTcpTuple() {
		try {
			int source_port = packetData.getUnsignedShort();
			int dest_port = packetData.getUnsignedShort();
			long seq = packetData.getUnsignedInt();
			long ack = packetData.getUnsignedInt();
			int headerLengthAndFlags = packetData.getUnsignedShort();
			int headerLength = ((headerLengthAndFlags >> 12) & 0x0f) * 4; 
			int flags = (headerLengthAndFlags & 0x0fff);
			int window = packetData.getUnsignedShort();
			int checksum = packetData.getUnsignedShort();
			int urgentPtr = packetData.getUnsignedShort();
			skipTcpOptions(headerLength);
			packetData.discardReadBytes();
			
			// (source_port, dest_port, seq, ack, headerLength, flags, window, urgent, data)
			Tuple tcpTuple = tupleFactory.newTuple(TCP_TUPLE_SIZE);
			tcpTuple.set(0, source_port);
			tcpTuple.set(1, dest_port);
			tcpTuple.set(2, seq);
			tcpTuple.set(3, ack);
			tcpTuple.set(4, headerLength);
			tcpTuple.set(5, flags);
			tcpTuple.set(6, window);
			tcpTuple.set(7, urgentPtr);
			tcpTuple.set(8, payloadToDataByteArray());
			return tcpTuple;
			
		} catch (BufferUnderflowException e) {
			return null;
		} catch (ExecException e) {
			return null;
		}
	}

	private void skipTcpOptions(int headerLength) {
		if (headerLength <= 20)
			return;

		int optionLength = headerLength - 20;

		for (int i = 0; i < optionLength; i++)
			packetData.get();

		if ((optionLength % 4) == 0)
			return;

		for (int i = 0; i < (optionLength % 4); i++)
			packetData.get();
		
	}

	protected Tuple parseUdpTuple() {
		try {
			int source_port = packetData.getUnsignedShort();
			int dest_port = packetData.getUnsignedShort();
			int length = packetData.getUnsignedShort();
			int checksum = packetData.getUnsignedShort();
			
			packetData.discardReadBytes();
			
			// (source_port, dest_port, totalLength, data)
			Tuple udpTuple = tupleFactory.newTuple(UDP_TUPLE_SIZE);
			udpTuple.set(0, source_port);
			udpTuple.set(1, dest_port);
			udpTuple.set(2, length);
			udpTuple.set(3, payloadToDataByteArray());
			return udpTuple;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (ExecException e) {
			return null;
		}
	}

	protected Tuple parseIcmpTuple() {
		try {
			int type = packetData.get();
			int code = packetData.get();
			int checksum = packetData.getShort();
			long other = packetData.getUnsignedInt();
			
			packetData.discardReadBytes();
			// (type, code, other, data)
			Tuple icmpTuple = tupleFactory.newTuple(ICMPV4_TUPLE_SIZE);
			icmpTuple.set(0, type);
			icmpTuple.set(1, code);
			icmpTuple.set(2, other);
			icmpTuple.set(3, payloadToDataByteArray()); // data
			
			return icmpTuple;
		} catch (BufferUnderflowException e) {
			return null;
		} catch (ExecException e) {
			return null;
		}
	}

	protected Tuple parseIpv4Tuple() {
		try {
			byte b = packetData.get();
			short s = (short) (b & 0x00ff);
			int ihl = (int) ((s & 0x000f) * 4);
			int tos = (packetData.get() & 0xff);
			int totalLength = (packetData.getUnsignedShort());
			int id = (packetData.getUnsignedShort());
			short flagsAndFragmentOffset = packetData.getShort();
			int flags = (int) (((short) ((flagsAndFragmentOffset >> 13) & 0x0007)) & 0xffff);
			int fragmentOffset = (int) ((short) (flagsAndFragmentOffset & 0x1fff) & 0xffff);
			int ttl = (packetData.get() & 0xff);
			ipProtocol = (packetData.get() & 0xff);
			int headerChecksum = (packetData.getUnsignedShort());
			String sourceAddress = getIpString();
			String destinationAddress = getIpString();
			skipIpv4Options(ihl);
			
			/* need discard */
			packetData.discardReadBytes();
			
			// (ihl, tos, totalLength, id, flags, fragmentOffset, ttl, protocol, source, destination, data)
			Tuple ipv4Tuple = tupleFactory.newTuple(IPV4_TUPLE_SIZE);
			ipv4Tuple.set(0, ihl);
			ipv4Tuple.set(1, tos);
			ipv4Tuple.set(2, totalLength);
			ipv4Tuple.set(3, id);
			ipv4Tuple.set(4, flags);
			ipv4Tuple.set(5, fragmentOffset);
			ipv4Tuple.set(6, ttl);
			ipv4Tuple.set(7, ipProtocol);
			ipv4Tuple.set(8, sourceAddress);
			ipv4Tuple.set(9, destinationAddress);
			ipv4Tuple.set(10, null); // data
			return ipv4Tuple;
			
		} catch (BufferUnderflowException e) {
			return null;
		} catch (ExecException e) {
			return null;
		}
	}

	private String getIpString() {
		byte[] ip = new byte[4];
		packetData.gets(ip, 0, 4);
		return String.format("%d.%d.%d.%d", (ip[0] & 0xff), (ip[1] & 0xff), (ip[2] & 0xff), (ip[3] & 0xff));
	}

	private void skipIpv4Options(int ihl) {
		if (ihl <= 20)
			return;

		int optionLength = ihl - 20;

		for (int i = 0; i < optionLength; i++)
			packetData.get();

		if ((optionLength % 4) == 0)
			return;

		for (int i = 0; i < optionLength % 4; i++)
			packetData.get();
		
	}

	protected Tuple parseEthernetTuple() {
		try {
			String destination = getMacAddress();
			String source = getMacAddress();
			ethertype = packetData.getUnsignedShort();
			packetData.discardReadBytes();
			
			Tuple ethernentTuple = tupleFactory.newTuple(ETHERNET_TUPLE_SIZE);
			
			// (source, destination, ethertype, data)
			ethernentTuple.set(0, source);
			ethernentTuple.set(1, destination);
			ethernentTuple.set(2, ethertype);
			ethernentTuple.set(3, null); // data
			return ethernentTuple;
		} catch (BufferUnderflowException e1) {
			return null;
		} catch (ExecException e) {
			return null;
		}
	}

	private String getMacAddress() {
		byte[] mac = new byte[6];
		packetData.gets(mac, 0, 6);
		return String.format("%02X:%02X:%02X:%02X:%02X:%02X", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
	}

	protected Tuple parseFrameTuple() {
		Tuple frameTuple = tupleFactory.newTuple(FRAME_TUPLE_SIZE);
		double timestamp = (double)(packetHeader.getTsSec() + packetHeader.getTsUsec()/1000000.0);
		int length = packetHeader.getOrigLen();
		String source = null;
		try {
			frameTuple.set(0, timestamp);
			frameTuple.set(1, source);
			frameTuple.set(2, length);
		} catch (ExecException e) {
			return null;
		}
		
		return frameTuple;
	}
	
	private DataByteArray payloadToDataByteArray() {
		int readableBytes = packetData.readableBytes();
		if (readableBytes == 0){
			return null;
		} else {
			byte[] payload = new byte[readableBytes];
			packetData.gets(payload);
			DataByteArray data = new DataByteArray(payload);
			return data;
		}
	}


}
