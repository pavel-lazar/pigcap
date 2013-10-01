package pigcap.udf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TcpFlowReconstruction extends EvalFunc<Tuple> {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Double lastTimestamp = null;
	private Long lastSeq = null;
	private Long nextExpectedSeq = null;
	private ArrayList<DataByteArray> dataSegements = null;
	double flowDuration = 0;
	Double flowStartTime;
	DataByteArray flowData = null;
	int flowDataSize = 0;

	@Override
	public Tuple exec(Tuple input) throws IOException {
		DataBag segments = (DataBag) input.get(0);
		long numberOfSegments = segments.size();
		if (numberOfSegments == 0) {
			return null;
		} else if (numberOfSegments == 1) {
			// segment = (timestamp, seq, data)
			Tuple segment = segments.iterator().next();
			flowStartTime = (Double) segment.get(0);
			flowData = (DataByteArray) segment.get(2);
		} else {
			for (Tuple segment : segments) {
				lastTimestamp = (Double) segment.get(0);
				Long currentSeq = (Long) segment.get(1);
				if (flowStartTime == null) {
					// first segment
					flowStartTime = lastTimestamp;
					lastSeq = currentSeq;
					dataSegements = new ArrayList<DataByteArray>(
							(int) numberOfSegments);
				} else {
					if (currentSeq.equals(lastSeq)) {
						// duplicate
						continue;
					} else if (currentSeq.compareTo(lastSeq) < 0) {
						throw new IOException(
								String.format(
										"input TCP segments series is not sorted by sequence number (%d < %d)",
										currentSeq, lastSeq));
					} else if (currentSeq.compareTo(nextExpectedSeq) != 0) {
						// Segments lost in TCP flow or something else really
						// strange happened
						//warn("segment lost in session or something else realy strange", PigWarning.UDF_WARNING_1);
						return null;
					} else {
						lastSeq = currentSeq;
					}
				}
				DataByteArray segmentData = (DataByteArray) segment.get(2);
				dataSegements.add(segmentData);
				int segmentDataSize = segmentData.size();
				flowDataSize += segmentDataSize;
				nextExpectedSeq = lastSeq + segmentDataSize;
			}
			flowData = buildFullFlowData();
			flowDuration = lastTimestamp - flowStartTime;
		}

		Tuple flow = tupleFactory.newTuple(3);
		flow.set(0, flowStartTime);
		flow.set(1, flowDuration);
		
		flow.set(2, flowData);
		return flow;
	}

	private DataByteArray buildFullFlowData() {
		byte[] fullData = new byte[flowDataSize];
		for (int i = 0; i < fullData.length;) {
			for (DataByteArray dataSegment : dataSegements) {
				byte[] data = dataSegment.get();
				for (int j = 0; j < data.length; j++) {
					fullData[i] = data[j];
					i++;
				}
			}
		}

		return new DataByteArray(fullData);
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);

			if (inputFieldSchema.type != DataType.BAG) {
				throw new RuntimeException("Expected a BAG as input");
			}

			Schema inputBagSchema = inputFieldSchema.schema;

			if (inputBagSchema.getField(0).type != DataType.TUPLE) {
				throw new RuntimeException(
						String.format(
								"Expected input bag to contain a TUPLE, but instead found %s",
								DataType.findTypeName(inputBagSchema
										.getField(0).type)));
			}

			Schema inputTupleSchema = inputBagSchema.getField(0).schema;

			if (inputTupleSchema.getField(0).type != DataType.DOUBLE) {
				throw new RuntimeException(
						String.format(
								"Expected first element of tuple to be a DOUBLE (timestamp), but instead found %s",
								DataType.findTypeName(inputTupleSchema
										.getField(0).type)));
			}

			if (inputTupleSchema.getField(1).type != DataType.LONG) {
				throw new RuntimeException(
						String.format(
								"Expected second element of tuple to be a LONG (sequance number), but instead found %s",
								DataType.findTypeName(inputTupleSchema
										.getField(0).type)));
			}

			if (inputTupleSchema.getField(2).type != DataType.BYTEARRAY) {
				throw new RuntimeException(
						String.format(
								"Expected third element of tuple to be a BYTEARRAY (segment data), but instead found %s",
								DataType.findTypeName(inputTupleSchema
										.getField(0).type)));
			}
			
			Schema outputSchema = new Schema();
			outputSchema.add(new FieldSchema("startTime", DataType.DOUBLE));
			outputSchema.add(new FieldSchema("duration", DataType.DOUBLE));
			outputSchema.add(new FieldSchema("data", DataType.BYTEARRAY));
			
			return outputSchema;
			
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
