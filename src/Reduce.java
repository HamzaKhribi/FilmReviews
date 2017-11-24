import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, Text> {


	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		

		/**** Connect to MongoDB ****/
		// Since 2.10.0, uses MongoClient
		MongoClient mongo = new MongoClient("localhost", 27017);

		/**** Get database ****/
		// if database doesn't exists, MongoDB will create it for you
		DB db = mongo.getDB("testdb");

		/**** Get collection / table from 'testdb' ****/
		// if collection doesn't exists, MongoDB will create it for you
		DBCollection table = db.getCollection("sentimentByDate");
		
			try {

				Iterator<IntWritable> i = values.iterator();
				int count = 0;
				
				while (i.hasNext())
					count += i.next().get();
				
				Text resultDate = new Text(
						DateConverter.monthToReadableDate(key.toString()));
				
				context.write(resultDate, new Text(count + " Positives"));

				/**** Insert ****/
				// create a document to store key and value
				BasicDBObject document = new BasicDBObject();
				document.put("Date", resultDate.toString());
				document.put("Rate", count+"Positives");
				
				table.insert(document);

			} finally {
				
			}
			

		

	}
}