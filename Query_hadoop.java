import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Query_hadoop {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{

   
	public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

		String queryLine = value.toString();
		String tokens[] = queryLine.split(":");
		if(tokens.length != 2) return;

		String queryID = tokens[0].trim();
		String queryText = tokens[1].trim();
		//System.out.println(queryText);	

		StringTokenizer itr = new StringTokenizer(queryText);
		Path pt=new Path("/user/hduser/util/stop-word-list.txt");
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line=br.readLine();
		Set<String> a = new HashSet<String>();
		while (line != null){
			a.add(line);
			line=br.readLine();
		}

		String temp;
		HashSet<String> CommonDocuments = new HashSet<String>();
		Vector<String> words = new Vector<String>();

		while (itr.hasMoreTokens()) {
			temp=itr.nextToken();
		
			temp = temp.toLowerCase().replaceAll("[^a-zA-Z]","").trim();
			if(!temp.equals("")) {
				if(!a.contains(temp)){
					pt=new Path("/user/hduser/Index/Words/"+ temp);
					if(fs.isDirectory(pt)) {
						//System.out.println(temp + ">>");
						FileStatus files[] = fs.listStatus(pt);
					
						HashSet<String> hset = new HashSet<String>();
						for(int i=0;i<files.length;i++) {
							String docName = files[i].getPath().getName();
							//System.out.println(docName);
							hset.add(docName);		
						}
					
						if(CommonDocuments.size() == 0) {
							CommonDocuments.addAll(hset);
						}
						else {
							CommonDocuments.retainAll(hset);
						}

						words.add(temp);
					}
					else {
						words.remove(temp);
						context.write(new Text(queryID), new Text("Not Found"));
						return;	
					}

	
				}
			}	
		
	      	}

		Iterator<String> docItr = CommonDocuments.iterator();
		//System.out.println("Common Documents >>");	
		while (docItr.hasNext()) {
			temp=docItr.next();
			//System.out.println(temp);
			
			Vector<String> posList = new Vector<String>();
			for(int i=0;i<words.size();i++) {
				pt=new Path("/user/hduser/Index/Words/"+ words.get(i) +"/"+ temp);
				br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				//System.out.println("word="+words.get(i));

				Vector<String> tmpList = new Vector<String>();
				while ((line=br.readLine()) != null){
					line = line.trim();
					int size = posList.size();
					if(i != 0) {
						for(int j=0;j<posList.size();j++) {
							tmpList.add(posList.get(j) + "_" + line);      //Later: improvisation
						}
					}
					else {
						tmpList.add(line);
					}
				}
				posList.removeAllElements();
				posList.addAll(tmpList); 
			}
		
			//calculate result
			//algorithm
			String posToken[];
			for(int i=0;i<posList.size();i++) {

				posToken = posList.get(i).split("_");
				int val1 = Integer.parseInt(posToken[0]) + (words.get(0).length())*2; 				
				boolean flag = true;
				for(int j=1;j<posToken.length;j++) {
					int val2 = Integer.parseInt(posToken[j]);
					int diff = val2-val1;
					if(!(diff >= 2 && diff <= 16)) {      //threshold:  min=2   max=16
						flag = false;
						break;
					}
					val1 = val2 + (words.get(j).length())*2;
				}
				if(flag) {
					//System.out.println("Found: " + posList.get(i));
					String posDoc = posList.get(i).split("_")[0];
					context.write(new Text(queryID), new Text("Found in " + temp + " at " + posDoc));
				}
			}
			
		}

	}
  }

  public static class OutputWriter
       extends Reducer<Text,Text,Text,Text> {
    

	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	
		for (Text val : values) {
       			context.write(key, val);
		}
		
	}
 }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Search Phrase");
    job.setJarByClass(Query_hadoop.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(OutputWriter.class);
    job.setReducerClass(OutputWriter.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}





