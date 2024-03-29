package pigcap.packet;

import java.util.Date;

/**
 * Packet header contains metadata for actual packet data. It doesn't mean
 * protocol header.
 * 
 * @author pavel
 * 
 */
public class PacketHeader {
	/**
	 * timestamp seconds
	 */
	private int tsSec;

	/**
	 * timestamp microseconds
	 */
	private int tsUsec;

	/**
	 * number of octets of packet saved in file
	 */
	private int inclLen;

	/**
	 * actual length of packet
	 */
	private int origLen;

	public PacketHeader(int tsSec, int tsUsec, int inclLen, int origLen) {
		this.tsSec = tsSec;
		this.tsUsec = tsUsec;
		this.inclLen = inclLen;
		this.origLen = origLen;
	}

	/**
	 * Deep copy constructor
	 * 
	 * @param source
	 *            the original
	 */
	public PacketHeader(PacketHeader source) {
		this.tsSec = source.tsSec;
		this.tsUsec = source.tsUsec;
		this.inclLen = source.inclLen;
		this.origLen = source.origLen;
	}

	public Date getDate() {
		return new Date(getTsSec() * 1000L + getTsUsec());
	}

	public int getTsSec() {
		return tsSec;
	}

	public int getTsUsec() {
		return tsUsec;
	}

	public int getInclLen() {
		return inclLen;
	}

	public int getOrigLen() {
		return origLen;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + inclLen;
		result = prime * result + origLen;
		result = prime * result + tsSec;
		result = prime * result + tsUsec;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PacketHeader other = (PacketHeader) obj;
		if (inclLen != other.inclLen)
			return false;
		if (origLen != other.origLen)
			return false;
		if (tsSec != other.tsSec)
			return false;
		if (tsUsec != other.tsUsec)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return String.format("%s (%d.%ds) - %d bytes", getDate(), tsSec, tsUsec, inclLen);
	}

}
