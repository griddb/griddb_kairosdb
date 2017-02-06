package sample;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.net.telnet.TelnetClient;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

// Put timeseries data and execute range query
public class Sample {

    static final long queryWidth = 1 * 1000;

    static final String kairos_ip_address = "127.0.0.1";//"localhost";

	// using Telnet API
    void append() throws Exception {
        // create telnet client
        final TelnetClient telnet = new TelnetClient();

        // connect server
        telnet.connect(kairos_ip_address, 4242);

        final OutputStream ostream = telnet.getOutputStream();
        final Writer writer = new OutputStreamWriter(ostream);

        final String metric = String.format("metric0");
    	long t = System.currentTimeMillis() - queryWidth;
        long val;   	
        String tags;

    	{
    		val = 1;
    		tags = String.format("host=AAA");
    		
	    	String data = "put " + metric + " " + t + " " + val + " " + tags + "\n";
	    	System.out.println(data);

	        writer.write(data);
	        writer.flush();
    	}
    	{
    		t += queryWidth;
    		val = 2;
    		tags = String.format("host=BBB");
    		
	    	String data = "put " + metric + " " + t + " " + val + " " + tags + "\n";
	    	System.out.println(data);

	        writer.write(data);
	        writer.flush();
    	}
    	
        // disconnect network
        telnet.disconnect();
    }

	// using REST API
	void query() throws URISyntaxException, IOException, ParseException {
        final String kairosdbServer = "http://" + kairos_ip_address + ":8080";

        final HttpClient client = new HttpClient(kairosdbServer);

        long latestTimestamp;
        {
            final SimpleDateFormat sf =
                    new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            final Date latestDate = sf.parse("9999-12-31 00:00:00.000");

            final QueryBuilder builder1 = QueryBuilder.getInstance();

            final QueryMetric metric = builder1.addMetric(String.format("metric0"));
            metric.setLimit(1);
            metric.setOrder(QueryMetric.Order.DESCENDING);

            builder1.setStart(new Date(0))
                    .setEnd(latestDate);

            final QueryResponse response = client.query(builder1);
        	latestTimestamp = checkResponse(response);
        }
        {
            long startTimestamp = latestTimestamp - queryWidth;

            final QueryBuilder builder1 = QueryBuilder.getInstance();

            builder1.setStart(new Date(startTimestamp))
                    .setEnd(new Date(latestTimestamp))
                    .addMetric(String.format("metric0"))
                    .addTag("host", String.format("AAA"));

            final QueryResponse response = client.query(builder1);
			latestTimestamp = checkResponse(response);
        }
    }

	long checkResponse(QueryResponse res) throws IOException {
            final List<Queries> queries = res.getQueries();
			//System.out.println("queries size = " + queries.size());
            if (queries.isEmpty()) {
                throw new IOException("Queries empty");
            }
			//System.out.println("results size = " + queries.get(0).getResults().size());
            if (queries.get(0).getResults().isEmpty()) {
                throw new IOException("Result empty");
            } else {
            	Results result = queries.get(0).getResults().get(0);
            	//System.out.println("datapoints size = " + result.getDataPoints().size());
            	System.out.println(
            		"tagName:" + result.getName() +
            		" tags:" + result.getTags() +
            		"\n\t(first result)" + result.getDataPoints().get(0).toString() + "\n");
            }
            if (queries.get(0).getResults().get(0).getDataPoints().isEmpty()) {
                throw new IOException("DataPoint empty");
            }
		return queries.get(0).getResults().get(0).getDataPoints()
                    .get(0).getTimestamp();
	}
	
    public Sample() {
    }

    public static void main(final String[] args) {
    	Sample sample = new Sample();
    	try {
    		sample.append();
    		sample.query();
    	} catch (final Exception ex) {
            System.out.println("Exception");
            ex.printStackTrace();
        }
    }
}
