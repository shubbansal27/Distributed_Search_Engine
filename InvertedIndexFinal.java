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


public class InvertedIndexFinal {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    private Text word = new Text();
    private Text filename = new Text(); 
    private String stopWords[] = {"a","about","above",
                                "across","after","afterwards","again","against","all","almost","alone","along","already","also",
				"although","always","am","among","amongst","amoungst","amount","an","and","another","any","anyhow",
				"anyone","anything","anyway","anywhere","are","around","as","at","back","be","became","because","become",
				"becomes","becoming","been","before","beforehand","behind","being","below","beside","besides","between",
				"beyond","bill","both","bottom","but","by","call","can","cannot","cant","co","computer","con","could",
				"couldnt","cry","de","describe","detail","do","done","down","due","during","each","eg","eight","either",
				"eleven","else","elsewhere","empty","enough","etc","even","ever","every","everyone","everything","everywhere",
				"except","few","fifteen","fify","fill","find","fire","first","five","for","former","formerly","forty","found",
				"four","from","front","full","further","get","give","go","had","has","hasnt","have","he","hence","her","here",
				"hereafter","hereby","herein","hereupon","hers","herse","him","himse","his","how","however","hundred","i","ie","if","in","inc","indeed","interest","into","is","it","its","itse","keep","last","latter","latterly","least","less","ltd","made","many","may","me","meanwhile","might","mill","mine","more","moreover","most","mostly","move","much","must","my","myse","name","namely","neither","never","nevertheless","next","nine","no","nobody","none","noone","nor","not","nothing","now","nowhere","of","off","often","on","once","one","only","onto","or","other","others","otherwise","our","ours","ourselves","out","over","own","part","per","perhaps","please","put","rather","re","same","see","seem","seemed","seeming","seems","serious","several","she","should","show","side","since","sincere","six","sixty","so","some","somehow","someone","something","sometime","sometimes","somewhere","still","such","system","take","ten","than","that","the","their","them","themselves","then","thence","there","thereafter","thereby","therefore","therein","thereupon","these","they","thick","thin","third","this","those","though","three","through","throughout","thru","thus","to","together","too","top","toward","towards","twelve","twenty","two","un","under","until","up","upon","us","very","via","was","we","well","were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","with","within","without","would","yet","you","your","yours","yourself","yourselves"};	
    private Set<String> a = null;	
	

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
	filename = new Text(filenameStr);
	
	
	/*Path pt=new Path("/user/hduser/util/stop-word-list.txt");
        FileSystem fs = FileSystem.get(new Configuration());
		
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	String line;
        line=br.readLine();
	*/	

	if(a == null) {	
		a = new HashSet<String>();
        	for(int i=0;i<stopWords.length;i++){
			a.add(stopWords[i]);
		}
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
    job.setJarByClass(InvertedIndexFinal.class);
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





