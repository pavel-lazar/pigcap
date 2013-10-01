package pigcap.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class ParseHttpRequestsFromString  extends EvalFunc<DataBag>{
	private final Pattern requestMessageHeader = Pattern.compile("(?:GET|POST|OPTIONS|HEAD|PUT|DELETE|TRACE|CONNECT)\\s");
	private String data;
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		// TODO: make the code work with pure bytearray
		
		data = (String) input.get(0);
		DataBag bagResult = BagFactory.getInstance().newDefaultBag();
		// make the data point to the start of http request header
		Matcher matcher = requestMessageHeader.matcher(data);
		if (matcher.find()) {
			data = data.substring(matcher.start());
		} else {
			return bagResult;
		}
		
		while ((data != null) && (data.length() > 0)) {
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
		String[] splited = data.split("\r\n\r\n",2);
		String headers = splited[0];
		data = splited.length == 2 ? splited[1] : null;
		int isHeaderFull = data != null ? 1 : 0; // we use int because pig has no boolean type
		String[] splitedHeaders = headers.split("\r\n");
		String requestLine = splitedHeaders[0];
		String[] splitedRequestLine = requestLine.split(" ");
		if (splitedRequestLine.length != 3) {
			return null;
		} 
		String method = splitedRequestLine[0];
		String uri = splitedRequestLine[1];
		String version = splitedRequestLine[2];
		Map<String, String> httpHeaders = new HashMap<String, String>(splitedHeaders.length - 1);
		for (int i = 1; i < splitedHeaders.length; i++) {
			String headerLine = splitedHeaders[i];
			String[] splitedHeader = headerLine.split(":\\s",2);
			if (splitedHeader.length != 2) {
				break;
			} else {
				httpHeaders.put(splitedHeader[0], splitedHeader[1]);
			}
		}
		
		DataByteArray contentData = null;
		if ((data != null) && (httpHeaders.containsKey("Content-Length"))) {
			try {
				int length = Math.min(Integer.parseInt(httpHeaders.get("Content-Length")), data.length());
				contentData = new DataByteArray(data.substring(0, length));
				data = data.substring(length);
			} catch (NumberFormatException e) {
				//do nothing
			}
		}
		
		Tuple result = tupleFactory.newTuple(6);
		try {
			result.set(0, isHeaderFull);
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
	
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);

			if (inputFieldSchema.type != DataType.CHARARRAY) {
				throw new RuntimeException(String.format("Expected a CHARARRAY as input , but instead found %s",
								DataType.findTypeName(inputFieldSchema.type)));
			}
		
			Schema bagSchema = new Schema();
			bagSchema.add(new FieldSchema("isHeaderFull", DataType.INTEGER));
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
