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


public class InvertedIndex {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    private Text word = new Text();
    private Text filename = new Text();
   
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
	filename = new Text(filenameStr);
	
	
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
	Text fileOffset = new Text(filenameStr);
	long wo = key.get();
	
	while (itr.hasMoreTokens()) {
		temp=itr.nextToken();
		
		word.set(temp.toLowerCase().replaceAll("[^a-zA-Z]",""));
		if(!word.toString().trim().equals("")) {
			if(!a.contains(word.toString())){
				fileOffset.set(filename.toString()+ "_" +wo);
				word.set(word.toString() + "_" + fileOffset.toString());			
				context.write(word, new Text(""));
			}
		}	
        wo=wo + 2*(temp.length()+1);	
      }
    }
  }

  public static class PostingListReducer
       extends Reducer<Text,Text,Text,Text> {
    
	private MultipleOutputs<Text,Text> mos;
        
	public void setup(Context context) {
		mos = new MultipleOutputs<Text,Text>(context);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	
		String t[] = key.toString().split("_");                           //here: some issue related to index out of bound
		if(t.length == 3) {
			mos.write("Index", t[2],new Text(""), "Words/"+t[0]+"/"+t[1]);
			context.write(new Text(t[0]), new Text(""));
		}	
	}
 }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PostingListReducer.class);
    job.setReducerClass(PostingListReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
    //conf.setNumMapTasks(30);
    //conf.setNumReduceTasks(3);
	

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    MultipleOutputs.addNamedOutput(job, "Index", TextOutputFormat.class, Text.class, Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}





