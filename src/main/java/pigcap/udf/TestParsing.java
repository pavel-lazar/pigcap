package pigcap.udf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;



public class TestParsing {

	public static void main(String[] args) {
		String s= "C:\\Temp\\pcaps\\raw_data.txt";
		try {
			InputStream in = new FileInputStream(s);
			int length = in.available();
			byte[] data = new byte[length];
			in.read(data);
			
			ParseHttpRequests requests = new ParseHttpRequests();
			DataBag bag = requests.exec(TupleFactory.getInstance().newTuple(new DataByteArray(data)));
			Tuple first = bag.iterator().next();
			System.out.println(bag.size());
			System.out.println(bag.toString());
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}
