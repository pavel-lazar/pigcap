package pigcap.util;

public class KMPMatch {
	public static int indexOf(byte[] data, byte[] pattern, int start, int end , int[] failure) throws IndexOutOfBoundsException {
		if ((start < 0) || (start > data.length) || (end > data.length) || (start > end)) {
			throw new IndexOutOfBoundsException();
		}
		int j = 0;
        if (data.length == 0) return -1;

        for (int i = start; i < end; i++) {
            while (j > 0 && pattern[j] != data[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == data[i]) { 
            	j++; 
            }
            if (j == pattern.length) {
                return i - pattern.length + 1;
            }
        }
		return -1;
	}
	
	public static int indexOf(byte[] data, byte[] pattern, int start, int end) throws IndexOutOfBoundsException {
		return indexOf(data, pattern, start, end, computeFailure(pattern));
	}
	
	public static int indexOf(byte[] data, byte[] pattern) throws IndexOutOfBoundsException {
		return indexOf(data, pattern, 0, data.length, computeFailure(pattern));
	}
	

	public static int[] computeFailure(byte[] pattern) {
        int[] failure = new int[pattern.length];

        int j = 0;
        for (int i = 1; i < pattern.length; i++) {
            while (j > 0 && pattern[j] != pattern[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == pattern[i]) {
                j++;
            }
            failure[i] = j;
        }

        return failure;
    }

	public static int indexOf(byte[] data, byte b, int start, int end) throws IndexOutOfBoundsException {
		if ((start < 0) || (start > data.length) || (end > data.length) || (start > end)) {
			throw new IndexOutOfBoundsException();
		}
		
		for (int i = start; i < end; i++) {
            if (data[i] == b){
            	return i;
            }
        }
		return -1;
	}
	
}
