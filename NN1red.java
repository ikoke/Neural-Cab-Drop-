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

public class NN1red
{

static int coun;
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

    public void setup(Context c) throws IOException
{
 



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
if(line<5)
for(u=0;u<parts.length;u++)
wt1[line][u]=Double.parseDouble(parts[u]);//+2;
else if(line<7)
for(u=0;u<parts.length;u++)
wt2[line-5][u]=Double.parseDouble(parts[u]);//+2;
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

fy[0]=(Math.abs(Double.parseDouble(ds[0]))-8)/.99;
fy[1]=(Double.parseDouble(ds[1])-41)/.99;

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

double x[]=new double[65];
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


cpar=coo[0];
String[] ps=cpar.split(",");
x[ind++]=(Math.abs(Double.parseDouble(ps[0]))-8)/.99;
x[ind++]=(Double.parseDouble(ps[1])-41)/.99;


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




int indi=61;

x[indi++]=m;
x[indi++]=d;
x[indi++]=hour;
x[indi]=catocl;

double Z2[]=new double[5];double A2[]=new double[6];
double Z3[]=new double[2],A3[]=new double[2];


//First hidden layer
    for(int iii=0;iii<5;iii++)
{
 double s=0;
   for(int j=0;j<=64;j++)
s+=wt1[iii][j]*x[j];

Z2[iii]=s;
A2[iii+1]=1/(1+Math.exp(0-Z2[iii]));
}

A2[0]=1;
//System.out.println("");
//Visible output layer
 for(int iii=0;iii<2;iii++)
{
 double s=0;
   for(int j=0;j<6;j++)
s+=wt2[iii][j]*A2[j];

Z3[iii]=s;
A3[iii]=s;
}

double rsx=A3[0]*Math.PI/180;
double rsy=A3[1]*Math.PI/180;
double rcx=fy[0]*Math.PI/180;
double rcy=fy[1]*Math.PI/180;

double aa=Math.pow(Math.abs(rsx-rcx),1)+Math.pow(Math.abs(rsy-rcy),1); 

//Start of backpropagation

//Difference at output layer

//calculation of delta at output layer
double d3[]=new double[2];
d3[0]=A3[0]-fy[0];
d3[1]=A3[1]-fy[1];

double temp[]=new double[6];

for(int y=0;y<6;y++)
{
double s=0;
for(int yy=0;yy<2;yy++)
    s+=wt2[yy][y]*d3[yy];

temp[y]=s;


}


//calculaion of delta at hidden layer
double d2[]=new double[6];
for(int y=0;y<6;y++)
d2[y]=temp[y]*A2[y]*(1-A2[y]);

double BD0[][]=new double[5][537];
double BD1[][]=new double[5][537];
String out="";

for(int y=0;y<5;y++)
{
for(int yy=0;yy<=64;yy++)
{BD0[y][yy]=x[yy]*d2[y];
out=out+BD0[y][yy]+" ";
}
}



for(int y=0;y<2;y++)
{
for(int yy=0;yy<6;yy++)
{BD0[y][yy]=A2[yy]*d3[y];
out=out+BD0[y][yy]+" ";
}
}
double wts=0;

//Calculating sum of weight parameters
for(int y=0;y<5;y++)
    for(int yy=0;yy<=64;yy++)
    wts+=Math.pow(wt1[y][yy],2);

for(int y=0;y<2;y++)
    for(int yy=0;yy<6;yy++)
    wts+=Math.pow(wt2[y][yy],2);


out=out+" "+aa+" "+wts+" "+1;


Text k=new Text("");
Text kkk=new Text(out);

       context.write(k,kkk);

       }
      }
    }


  public static class MidCombiner extends Reducer<Text,Text,Text,Text>
{
public void combine(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String sum ="";
      int count=0;
double sux=0.0,suy=0.0,sex=0.0,sey=0.0;
double v[]=new double[337],vv[]=new double[337];double lambda=.5,lamb1=.5;

for(int y=0;y<337;y++)
    vv[y]=0;

double p1=0,p2=0;


      for (Text val : values) {
        String va= val.toString();

        String comp[]=va.split(" ");
        count+=Integer.parseInt(comp[comp.length-1]);
        p1+=Double.parseDouble(comp[comp.length-3]);
        p2=Double.parseDouble(comp[comp.length-2]);

for(int y=0;y<337;y++)
    vv[y]+=Double.parseDouble(comp[y]);



      }

for(int y=0;y<337;y++)
sum+=vv[y]+" ";

      sum+=p1+" "+p2+" "+count;
      context.write(new Text(""), new Text(sum));
    }


}


  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
long itn;
double wt[]=new double[337];double oJt;
  public void setup(Context c) throws IOException
{
 
{




	String line;
				FileSystem f=FileSystem.get(c.getConfiguration());
URI[] cf=DistributedCache.getCacheFiles(c.getConfiguration());
Path p=new Path(cf[0].getPath());
 // BufferedReader b = new BufferedReader(new FileReader(cacheFiles[0].toString()));
BufferedReader b=new BufferedReader(new InputStreamReader(f.open(p)));
  String s;//int line=0;
int i=0;int ln=0;int ll=0;
while((s=b.readLine())!=null)
{
//System.out.println(s);
String parts[]=s.split("\t| ");
if(ln<5)
ll=65;
else if(ln<7)
ll=6;
else 
ll=1;

if(ln<7){
for(int u=0;u<ll;u++)
wt[i++]=Double.parseDouble(parts[u]);//+2;
}

if(ln==7)
itn=Long.parseLong(parts[0]);
//else  if(ln==8)
//{}
//oJt=Double.parseDouble(parts[0]);


ln++;

}

}


}










    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
   
      int counter=0;int flag=1;double nJt=0;
double sux=0.0,suy=0.0,sex=0.0,sey=0.0;
double v[]=new double[337],vv[]=new double[337];double lambda=.5,lamb1=0.02;
//double nJt=0;
for(int y=0;y<337;y++)
    vv[y]=0;
int count=0;
double wtsu=0;
      for (Text val : values) {
        String va= val.toString();
        String comp[]=va.split(" ");
        count+=Integer.parseInt(comp[comp.length-1]);
        wtsu=Double.parseDouble(comp[comp.length-2]);
        nJt+=Double.parseDouble(comp[comp.length-3]);
for(int y=0;y<337;y++)
    vv[y]+=Double.parseDouble(comp[y]);


      }
double Jt=(nJt/count)+(.001/(2.0*count)*wtsu);//(-1/count*nJt)+(.001/(2*count)*wtsu);

//if(Jt<oJt||itn<10)//&&itn!=0)
{

      for(int i=0;i<337;i++)
      {
      v[i]=vv[i]/count;

      if(i!=0&&i!=65&&i!=130&&i!=195&&i!=260&&i!=325&&i!=331)
        v[i]+=lamb1*wt[i];


      }

    

lambda=.1;//5;//.1;//.02;//.01;//1;//.01;//1;//10;//0.1;//0.01;
int li=0;
 
     counter=0;System.out.println("Loop"+li);
     double tempi[]=new double[337];double min=999999,max=0;

 for(int y=0;y<337;y++)
        {tempi[y]=wt[y]-lambda*v[y];
}

for(int y=0;y<337;y++)
    wt[y]=tempi[y];


li++;
    
}
//else if(Jt>oJt&&itn>10)
//flag=-1;
int tind=0;

double out[][]=new double[5][65];

for(int i=0;i<5;i++)
{for(int ii=0;ii<65;ii++)
    out[i][ii]=wt[tind++];
    String va="";
    for(int ii=0;ii<65;ii++)
    va=va+out[i][ii]+" ";
    context.write(new Text(va),new Text(""));
}

for(int i=0;i<2;i++)
{
for(int ii=0;ii<6;ii++)
    out[i][ii]=wt[tind++];
    String va="";
    for(int ii=0;ii<6;ii++)
    va=va+out[i][ii]+" ";
    context.write(new Text(va),new Text(""));



}
context.write(new Text(""+(itn+1)),new Text(""));
context.write(new Text(""+Jt),new Text(""));

   //   context.write(key, new Text(sum));



    }
  }

  public static void main(String[] args) throws Exception {
coun=0;//13;//0;
cln=50;
double ow[][]=new double[7][537];
double nw[][]=new double[7][537];
long oldflag,newflag;


String prevout="";double GLB=9999; int bestit=0;
int tc=50;
   while(coun!=3000)
{
System.out.println("Iteration= "+coun);
Path hdfsPath;
    String pat=args[1]+"/"+coun;
    Configuration conf = new Configuration();
FileSystem fs=FileSystem.get(conf);
 if (coun == 0)
{
				 hdfsPath= new Path(args[2]);
}
   else
 {
				hdfsPath= new Path(prevout+"/part-r-00000");
				
 }

FileStatus[] list=fs.globStatus(hdfsPath);

for(FileStatus st:list)
{
DistributedCache.addCacheFile(st.getPath().toUri(),conf);


}

FileSystem f=FileSystem.get(conf);

URI[] cf=DistributedCache.getCacheFiles(conf);
Path p=new Path(cf[0].getPath());
 
BufferedReader b=new BufferedReader(new InputStreamReader(f.open(p)));
  String s;int ln=0;
double oldJt, newJt=0;
//int ln=0;
while((s=b.readLine())!=null)
{
//System.out.println(s);
String temp[]=s.split("\t| ");

if(temp.length==1&&ln==7)
oldflag=Long.parseLong(temp[0]);
else if(temp.length==1&&ln==8)
oldJt=Double.parseDouble(temp[0]);
ln++;



}




    Job job = Job.getInstance(conf, "Neural");
    job.setJarByClass(NN1red.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
   job.setCombinerClass(MidCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //conf.setNumMapTasks(1000);
   FileInputFormat.setMaxInputSplitSize(job,4652881L);//807000L);






    FileInputFormat.addInputPath(job, new Path(args[0]));

   FileOutputFormat.setOutputPath(job, new Path(pat));  prevout=pat;

    job.waitForCompletion(true);



FileSystem fo1=FileSystem.get(new Configuration());
//if(coun>0)
Path p1=new Path(pat+"/part-r-00000");
BufferedReader br1=new BufferedReader(new InputStreamReader(fo1.open(p1)));
String inp="";ln=0;newflag=0;
while((inp=br1.readLine())!=null)
{
String temp[]=inp.split("\t| ");

if(ln==7)
newflag=Long.parseLong(temp[0]);
if(ln==8){
newJt=Double.parseDouble(temp[0]);System.out.println("Jthet="+newJt);}
ln++;

}

if(newJt<GLB)
{
GLB=newJt;
bestit=coun;
}

System.out.println("Best minima found till date is= "+GLB+" which was found on iteration="+bestit);



coun++;
}
  }
}

