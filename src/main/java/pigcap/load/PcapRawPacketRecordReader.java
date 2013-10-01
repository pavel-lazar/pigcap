package pigcap.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import pigcap.packet.PcapPacket;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.zip.GZIPInputStream;

public class PcapRawPacketRecordReader extends RecordReader<LongWritable, PcapPacket> {
    private LongWritable key = new LongWritable();
    private String pathString;
    private PcapFSDataInputStream pcapFileInputStream = null;
	private PcapPacket pcapPacket = null;
	private long fileLength = 0;
	private TaskAttemptContext taskAttemptContext;
	private static final PigStatusReporter statusReporter = PigStatusReporter.getInstance();
	
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        final Path filePath = fileSplit.getPath();
        fileLength = fileSplit.getLength();
        taskAttemptContext = context;
        createPcapFileInputStream(context, filePath);
    }

	private void createPcapFileInputStream(TaskAttemptContext context,
			final Path filePath) throws IOException {
		Configuration config = context.getConfiguration();
		FileSystem fs = FileSystem.get(filePath.toUri(), config);
		FSDataInputStream fsdis = fs.open(filePath);
		pathString = filePath.toString();

		if (pathString.endsWith(".gz")) {
			pcapFileInputStream = new PcapFSDataInputStream(
					new GZIPInputStream(fsdis), fileLength);
		} else if (pathString.endsWith(".bz") || pathString.endsWith(".bz2")) {
			pcapFileInputStream = new PcapFSDataInputStream(
					new CBZip2InputStream(fsdis), fileLength);
		} else {
			pcapFileInputStream = new PcapFSDataInputStream(fsdis, fileLength);
		}
	}

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public PcapPacket getCurrentValue() throws IOException, InterruptedException {
        return pcapPacket;
    }

    @Override
    public float getProgress() {
        if (fileLength == 0) {
        	return 0.0f;
        } else {
        	float currentOffset = pcapFileInputStream.getOffset();
        	return Math.min(1.0f, currentOffset / (float)(fileLength - currentOffset));
        }
    }

    @Override
    public void close() throws IOException {
    	if (pcapFileInputStream != null){
    		pcapFileInputStream.close();
    	}
    }

    @Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
    	try {
            pcapPacket = nextRawPacket();

            if (pcapPacket == null) {
            	if (pcapFileInputStream != null) {
            		pcapFileInputStream.close();
            	}
                return false;
            }

            
            long tv_sec = pcapPacket.getPacketHeader().getTsSec();
            long tv_usec = pcapPacket.getPacketHeader().getTsUsec();
            key.set(tv_sec * 1000 + tv_usec / 1000);
            return true;

        } catch (BufferUnderflowException ignored) {
        	if (pcapFileInputStream != null) {
        		pcapFileInputStream.close();
        	}
            return false;
        } catch (NegativeArraySizeException ignored) {
        	if (pcapFileInputStream != null) {
        		pcapFileInputStream.close();
        	}
            return false;
        } catch (IOException ignored) {
        	if (pcapFileInputStream != null) {
        		pcapFileInputStream.close();
        	}
            return false;
        }
	}

	private PcapPacket nextRawPacket() throws IOException {
        try {
        	// TODO: make sure it's the right place to use
            Counter counter = statusReporter.getCounter("PigCap", "nextPacket");
            counter.increment(1);
        } catch (NullPointerException ignored) {
            //???
        }
        
        // TODO: make sure it's the right place to use
        taskAttemptContext.progress();
        try {
            return pcapFileInputStream.getPacket();
        } catch (NegativeArraySizeException ignored) {
            return null;
        }
    }
}
