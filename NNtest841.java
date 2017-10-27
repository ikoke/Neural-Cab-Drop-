import java.io.*;//IOException;
import java.util.*;//StringTokenizer;
import java.util.StringTokenizer;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;//Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NNtest841
{

static int coun;
static double cdx[][]=new double[50][3];
static double cdy[][]=new double[50][3];

  //new int[100][3],cdy[][]=new int[100][3],
static int np[]=new int[50];
static int cln;
static double cx[][]=new double[50][3],cy[][]=new double[50][3],ncx[][]=new double[50][3],ncy[][]=new double[50][3];

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
static double wt1[][]=new double[5][65];
static double wt2[][]=new double[2][6];

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

/*setup function initialises the link weights*/
    public void setup(Context c) throws IOException
{
 


	//String line;
				FileSystem f=FileSystem.get(c.getConfiguration());
URI[] cf=DistributedCache.getCacheFiles(c.getConfiguration());
Path p=new Path(cf[0].getPath());
 // BufferedReader b = new BufferedReader(new FileReader(cacheFiles[0].toString()));
BufferedReader b=new BufferedReader(new InputStreamReader(f.open(p)));
  String s;int line=0;
while((s=b.readLine())!=null)
{
//System.out.println(s);
String parts[]=s.split("\t| ");int u;
if(line<5){
for(u=0;u<parts.length;u++)
wt1[line][u]=Double.parseDouble(parts[u]);}
else if(line<7){
for(u=0;u<parts.length;u++)
wt2[line-5][u]=Double.parseDouble(parts[u]);}

line++;


}
}

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String i=value.toString();//StringTokenizer itr = new StringTokenizer(value.toString());
      int ii=i.indexOf(',');
String id=i.substring(1,i.indexOf(",")-1);
      int fb=i.indexOf('[');double fy[]=new double[2];
      int cb=i.lastIndexOf(']');

     // if(fb!=-1&&cb!=-1&&(cb-fb)>=4)
      {
      String sc=i.substring(fb+1,cb-1);
String coo[]=sc.split("]");
      String fp=i.substring(0,fb-2);
      String[] ar=fp.split(",");
if(coo[coo.length-1].charAt(0)==',')
coo[coo.length-1]=coo[coo.length-1].substring(2);
else
coo[coo.length-1]=coo[coo.length-1].substring(1);
String ds[]=coo[coo.length-1].split(",");
fy[0]=Double.parseDouble(ds[0]);
fy[1]=Double.parseDouble(ds[1]);

for(int in=1;in<=ar.length;in++)
{

if(ar[in-1].length()<3)
ar[in-1]="";
else
ar[in-1]=ar[in-1].substring(1,ar[in-1].length()-1);

}

int ct[]={0,0,0};int ost[]=new int[63];int tn[]=new int[448];
ct[(int)ar[1].charAt(0)-65]=1;
Calendar c=Calendar.getInstance();
c.setTimeInMillis(Long.parseLong(ar[5])*1000L);
//int m[]={0,0,0,0,0,0,0,0,0,0,0,0};
//int d[]={0,0,0,0,0,0,0};
double m=c.get(Calendar.MONTH)/11.0;//m[Calendar.MONTH-1]=1;
double d=(c.get(Calendar.DAY_OF_WEEK)-1)/6.0;//d[Calendar.DAY_OF_WEEK-1]=1;
double hour=c.get(Calendar.HOUR_OF_DAY)/23.0;
double catocl;

if(ar[1].equals("A"))
catocl=0;
else if(ar[2].equals("B"))
catocl=1/2.0;
else
catocl=1;

String start=i.substring(fb+2,i.indexOf("]"));//;cb-1);
String pp[]=start.split(",");
double sx=Double.parseDouble(pp[0]);
double sy=Double.parseDouble(pp[1]);

double x[]=new double[65];//Remember to expand x.'s length to max no of points possible, and initialise all to 0
int ind=0;
for(int cc=0;cc<65;cc++)
x[cc]=0;
x[0]=1;


ind=1;String cpar;



for(int ci=0;ci<coo.length;ci++)
{

    //  if(ci!=0)
//cpar=coo[ci];
if(coo[ci].charAt(0)==',')
coo[ci]=coo[ci].substring(2);
else
coo[ci]=coo[ci].substring(1);

}

if(coo.length>=30)
{
cpar=coo[0];
String[] ps=cpar.split(",");
x[ind++]=(Math.abs(Double.parseDouble(ps[0]))-8)/.99;
//x[ind-1]=x[ind-1]*2-1;
x[ind++]=(Double.parseDouble(ps[1])-41)/.99;
//x[ind-1]=x[ind-1]*2-1;


int df=(coo.length-2)/28;
int ri=df;


for(int ci=0;ci<28;ci++)
{
cpar=coo[ri];
ps=cpar.split(",");
x[ind++]=(Math.abs(Double.parseDouble(ps[0]))-8)/.99;
x[ind++]=(Double.parseDouble(ps[1])-41)/.99;

ri+=df;

}

cpar=coo[coo.length-2];
ps=cpar.split(",");
x[ind++]=(Math.abs(Double.parseDouble(ps[0]))-8)/.99;
x[ind++]=(Double.parseDouble(ps[1])-41)/.99;

}

else
{

for(int ain=0;ain<(coo.length-1);ain++)
{
cpar=coo[ain];
String []ps=cpar.split(",");
x[ind++]=(Math.abs(Double.parseDouble(ps[0]))-8)/.99;
x[ind++]=(Double.parseDouble(ps[1])-41)/.99;


}


}





int indi=61;

x[indi++]=m;


x[indi++]=d;
x[indi++]=hour;
x[indi]=catocl;


double Z2[]=new double[5];double A2[]=new double[6];
double Z3[]=new double[2],A3[]=new double[2];

String output=id+" ";
//First hidden layer
    for(int iii=0;iii<5;iii++)
{
 double s=0;
   for(int j=0;j<=64;j++)
s+=wt1[iii][j]*x[j]*1.0;

Z2[iii]=s;
A2[iii+1]=1/(1+Math.exp(0-Z2[iii]));

}

A2[0]=1;

//Visible output layer
 for(int iii=0;iii<2;iii++)
{
 double s=0;
   for(int j=0;j<6;j++)
s+=wt2[iii][j]*A2[j];

Z3[iii]=s;
A3[iii]=Z3[iii];//1/(1+Math.exp(0-Z3[iii]));
}

String out="-"+((A3[0]*.99)+8)+","+((A3[1]*.99)+41)+"\t"+fy[0]+","+fy[1]; 
context.write(new Text(id), new Text(out));
//  context.write(new Text(output),new Text(""));     
      }
    }

}

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

double wt[]=new double[337];
  

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException 
{

/*
*/    
for(Text c:values)
{
String in=c.toString();
//if(!in.equals(""))
{
String[] p=in.split(" |\t");
String[] pred=p[0].split(",");
String[] act=p[1].split(",");

double px=Math.abs(Double.parseDouble(pred[0]));
double py=Double.parseDouble(pred[1]);

double ax=Math.abs(Double.parseDouble(act[0]));
double ay=Double.parseDouble(act[1]);


double rsx=px*Math.PI/180.0;
double rsy=py*Math.PI/180.0;
double rcx=ax*Math.PI/180.0;
double rcy=ay*Math.PI/180.0;

double aa=Math.pow(Math.sin((rcy-rsy)/2),2)+Math.cos(rsy)*Math.cos(rcy)*Math.pow(Math.sin((rcx-rsx)/2),2);
double cc=2*Math.atan2(Math.sqrt(aa),Math.sqrt(1-aa));
double dd=6371*cc;

//context.write(key,new Text(""+in));
context.write(key,new Text(""+in+" "+dd));}//,new Text("")); 
}
   //   context.write(key, new Text(sum));






  }
  }

  public static void main(String[] args) throws Exception 
{

String prevout="";
int tc=50;
  // while(coun!=4)
{
Path hdfsPath;
   
    Configuration conf = new Configuration();
FileSystem fs=FileSystem.get(conf);
 
				 hdfsPath= new Path(args[2]);

FileStatus[] list=fs.globStatus(hdfsPath);

for(FileStatus st:list)
{
DistributedCache.addCacheFile(st.getPath().toUri(),conf);


}

FileSystem f=FileSystem.get(conf);

URI[] cf=DistributedCache.getCacheFiles(conf);
Path p=new Path(cf[0].getPath());
 // BufferedReader b = new BufferedReader(new FileReader(cacheFiles[0].toString()));
BufferedReader b=new BufferedReader(new InputStreamReader(f.open(p)));
  String s;



    Job job = Job.getInstance(conf, "Neural");
    job.setJarByClass(NNtest841.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
   job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
FileInputFormat.setMaxInputSplitSize(job,4652881L);//807000L);



    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // prevout=pat;

    job.waitForCompletion(true);



}
  }
}

