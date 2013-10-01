package pigcap.load;

import pigcap.packet.PacketHeader;
import pigcap.packet.PcapPacket;
import pigcap.util.Buffer;
import pigcap.util.ByteOrderConverter;
import pigcap.util.ChainBuffer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PcapFSDataInputStream {
    private DataInputStream is;
    private int magic;
    private int tz;
    private long length;
    private int offset;

    public PcapFSDataInputStream(InputStream data, long length) throws IOException {
        is = new DataInputStream(data);
        readGlobalHeader();
        this.length = length;
        this.offset = 0;
    }

    public PcapPacket getPacket() throws IOException {
        return readPacket(magic);
    }

    public final void readGlobalHeader() throws IOException {
        int bytesToRead = 4 + 2 + 2 + 4 + 4 + 4 + 4;
        byte[] buffer = new byte[bytesToRead];
        is.readFully(buffer);
        offset += bytesToRead;
        ByteBuffer buf = ByteBuffer.wrap(buffer);

        this.magic = buf.getInt();
        short major = buf.getShort();
        short minor = buf.getShort();
        this.tz = buf.getInt();


        if (magic == 0xD4C3B2A1)
            tz = ByteOrderConverter.swap(tz);
    }


    private PcapPacket readPacket(int magicNumber) throws IOException {
        PacketHeader packetHeader = readPacketHeader(magicNumber);
        Buffer packetData = readPacketData(packetHeader.getInclLen());
        return new PcapPacket(packetHeader, packetData);
    }

    private PacketHeader readPacketHeader(int magicNumber) throws IOException {
        int bytesToRead = 4 + 4 + 4 + 4;
        byte[] buffer = new byte[bytesToRead];
        is.readFully(buffer);
        offset += bytesToRead;
        ByteBuffer buf = ByteBuffer.wrap(buffer);

        int tsSec = buf.getInt();
        int tsUsec = buf.getInt();
        int inclLen = buf.getInt();
        int origLen = buf.getInt();

        if (magicNumber == 0xD4C3B2A1) {
            tsSec = ByteOrderConverter.swap(tsSec);
            tsUsec = ByteOrderConverter.swap(tsUsec);
            inclLen = ByteOrderConverter.swap(inclLen);
            origLen = ByteOrderConverter.swap(origLen);
        }

        return new PacketHeader(tsSec, tsUsec, inclLen, origLen);
    }

    private Buffer readPacketData(int packetLength) throws IOException {
        byte[] packets = new byte[packetLength];
        is.readFully(packets);
        offset += packetLength;

        Buffer payload = new ChainBuffer();
        payload.addLast(packets);
        return payload;
    }

    public void close() throws IOException {
        is.close();
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }
    
    public int getMagic() {
    	return magic;
    }
    
    public int getTz() {
    	return tz;
    }
}
