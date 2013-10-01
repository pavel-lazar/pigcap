
package pigcap.util;

public class ByteOrderConverter {
	private ByteOrderConverter() {
	}

	public static int swap(int value) {
		int a = value;
		int b = (a >> 24) & 0xFF;
		int c = (a >> 8) & 0xFF00;
		int d = (a << 8) & 0xFF0000;
		int e = (a << 24) & 0xFF000000;
		return (b | c | d | e);
	}

	public static short swap(short value) {
		short a = value;
		short b = (short) ((a >> 8) & 0xFF);
		short c = (short) ((a << 8) & 0xFF00);
		return (short)(b | c);
	}
	
	public static long swap(long value){
		long v = 0;
		for (int i = 7; i >= 0; i--) {
			v |= ((value >> ((7 - i) * 8)) & 0xff);
			if(i ==0){
				break;
			}
			v <<= 8;
		}
		return v;
	}
}
