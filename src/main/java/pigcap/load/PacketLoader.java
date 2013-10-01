package pigcap.load;

import java.io.IOException;






import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;

import pigcap.packet.PacketRecord;
import pigcap.packet.PcapPacket;


/**
 * @author pavel
 *
 */
public class PacketLoader extends LoadFunc implements LoadMetadata {
	private static final Log LOG = LogFactory.getLog(PacketLoader.class);
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;

	@SuppressWarnings("rawtypes")
	public InputFormat getInputFormat() throws IOException {
		return new FileInputFormat<LongWritable, PcapPacket>() {
			@Override
			public RecordReader<LongWritable, PcapPacket> createRecordReader(
					InputSplit split, TaskAttemptContext context) {
				return new PcapRawPacketRecordReader();
			}

			/*
			 * Make sure each file is processed by a single mapper
			 * 
			 * @see
			 * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable
			 * (org.apache.hadoop.mapreduce.JobContext,
			 * org.apache.hadoop.fs.Path)
			 */
			@Override
			public boolean isSplitable(JobContext context, Path filename) {
				return false;
			}
		};
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	public Tuple getNext() throws IOException {
		if (reader == null) {
			return null;
		}

		try {
			// Read the next key-value pair from the record reader.
			// If it's finished return null
			while (reader.nextKeyValue()) {

				// Get the current value, we don't use the key.
				PcapPacket pcapPacket = (PcapPacket) reader.getCurrentValue();
				
				// incrCounter(PacketLoaderCounters.PacketsRead, 1L);
				PacketRecord packetRecord = new PacketRecord(pcapPacket);
				Tuple t = packetRecord.parseTupleFromPacket();
				
				if (t != null) {
					// incrCounter(PacketLoaderCounters.PacketsDecoded, 1L);
					return t;
				}
			}

			return null;

		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}
	}

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		this.reader = reader;	
	}

	// LoadMetadata interface
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		final String schema = "frame:tuple(ts:double, source:chararray, length:int), eth:tuple(src:chararray, dst:chararray, type:chararray, data:bytearray), ip:tuple(ihl:int, tos:int, length:int, id:int, flags:int, offset:int, ttl:int, protocol:int, src:chararray, dst:chararray, data:bytearray) , icmp:tuple(type:int, code:int, other:long, data:bytearray), udp:tuple(sport:int, dport:int, length:int, data:bytearray), tcp:tuple(sport:int, dport:int, seq:long, ack:long, hlen:int, flags:int, window:int, urgent:int, data:bytearray)";
		return new ResourceSchema(Utils.getSchemaFromString(schema));
	}

	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {

		// we don't implement yet
		return null;
	}

	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		// We don't have partitions
		return null;
	}

	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
		// We don't have partitions
		
	}
}
