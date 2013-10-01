package pigcap.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;



import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.MapKeysInfo;

import pigcap.util.KMPMatch;

public class ParseHttpRequests  extends EvalFunc<DataBag>{
	private byte[] data;
	private int offset = 0;
	private final byte[] CRLF = "\r\n".getBytes();
	private final int[] CRLF_FAILURE = KMPMatch.computeFailure(CRLF);
	private final byte[] METHOD_SEPERATOR = ": ".getBytes();
	private final int[] METHOD_SEPERATOR_FAILURE = KMPMatch.computeFailure(METHOD_SEPERATOR);
	private final String[] METHODS = {"GET", "POST", "OPTIONS", "HEAD", "PUT", "DELETE", "TRACE", "CONNECT"};
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		// TODO: make the code work with pure bytearray
		
		data = ((DataByteArray) input.get(0)).get();
		DataBag bagResult = BagFactory.getInstance().newDefaultBag();
		
		while (offset < data.length) {
			Tuple httpRequestTuple = getNextHttpRequestTuple();
			if (httpRequestTuple == null) {
				break;
			} else {
				bagResult.add(httpRequestTuple);
			}
		}
		
		return bagResult;
	}

	private Tuple getNextHttpRequestTuple() {
		int isFullHeaders = 0; // we use int because pig has no boolean type
		String method = copyFromByteArrayUntil(" ".getBytes(), KMPMatch.computeFailure(" ".getBytes()));
		if ((method == null) || !(Arrays.asList(METHODS).contains(method))) {
			return null;
		}
		String uri = copyFromByteArrayUntil(" ".getBytes(), KMPMatch.computeFailure(" ".getBytes()));
		String version = copyFromByteArrayUntil(CRLF, CRLF_FAILURE);
		if ((version == null) || !(version.startsWith("HTTP"))) {
			return null;
		}
		
		Map<String, String> httpHeaders = new HashMap<String, String>();
		while (offset < data.length) {
			if (offset == KMPMatch.indexOf(data, CRLF, offset, data.length, CRLF_FAILURE)) {
				// we have reached the end of the headers
				offset += CRLF.length;
				isFullHeaders = 1;
				break;
			}
			String methodName = copyFromByteArrayUntil(METHOD_SEPERATOR, METHOD_SEPERATOR_FAILURE);
			if (methodName == null){
				// the session was cut off in the middle of a method name. 
				// move the offset to the end
				offset = data.length;
			} else {
				String methodValue = copyFromByteArrayUntil(CRLF, CRLF_FAILURE);
				if (methodValue == null){
					// the session was cut of in the middle of the method value, cRsave what we can
					methodValue = new String(Arrays.copyOfRange(data, offset, data.length));
					offset = data.length;
				}
				httpHeaders.put(methodName, methodValue);
			}
			
		}
		
		DataByteArray contentData = null;
		if ((offset < data.length) && (httpHeaders.containsKey("Content-Length"))) {
			try {
				int length = Math.min(Integer.parseInt(httpHeaders.get("Content-Length")), data.length - offset);
				contentData = new DataByteArray(Arrays.copyOfRange(data, offset, offset+length));
				offset += length;
			} catch (NumberFormatException e) {
				//do nothing
			}
		}
		
		Tuple result = tupleFactory.newTuple(6);
		try {
			result.set(0, isFullHeaders);
			result.set(1, method);
			result.set(2, uri);
			result.set(3, version);
			result.set(4, httpHeaders);
			result.set(5, contentData);
		} catch (ExecException e) {
			return null;
		}
		
		return result;
	}
	
	private String copyFromByteArrayUntil(byte[] pattern, int[] failure) {
		int end = KMPMatch.indexOf(data, pattern, offset, data.length, failure);
		if (end == -1) {
			return null;
		}
		String string = new String(Arrays.copyOfRange(data, offset, end));
		offset = end + pattern.length;
		return string;
	}


	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);

			if (inputFieldSchema.type != DataType.CHARARRAY) {
				throw new RuntimeException(String.format("Expected a CHARARRAY as input , but instead found %s",
								DataType.findTypeName(inputFieldSchema.type)));
			}
		
			Schema bagSchema = new Schema();
			bagSchema.add(new FieldSchema("isFullHeaders", DataType.INTEGER));
			bagSchema.add(new FieldSchema("method", DataType.CHARARRAY));
			bagSchema.add(new FieldSchema("uri", DataType.CHARARRAY));
			bagSchema.add(new FieldSchema("version", DataType.CHARARRAY));
			bagSchema.add(new FieldSchema("headers", DataType.MAP));
			bagSchema.add(new FieldSchema("data", DataType.BYTEARRAY));

			return new Schema(new Schema.FieldSchema("request", bagSchema, DataType.BAG));

		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
