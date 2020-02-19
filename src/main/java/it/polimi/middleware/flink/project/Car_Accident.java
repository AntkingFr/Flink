package it.polimi.middleware.flink.project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import java.util.ArrayList;

import java.util.Date;
import java.util.Calendar;

import java.lang.Double;
import java.lang.Integer;

import java.text.SimpleDateFormat;
import java.text.ParseException;

public class Car_Accident{

    public static void main(String[] args) throws Exception{
	final ParameterTool params = ParameterTool.fromArgs(args);
	final String NYPD_Motor_Vehicle_Collisions = params.get("NYPD_Motor_Vehicle_Collisions", "files/NYPD_Motor_Vehicle_Collisions.csv");
	
	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
	final DataSet<Tuple8<String, String, Integer, String, String, String, String, String>> csvData = env
	    .readCsvFile(NYPD_Motor_Vehicle_Collisions)
	    .ignoreFirstLine()
	    .ignoreInvalidLines()
	    .includeFields("10100000000100000011111000000")
	    .types(String.class, String.class, Integer.class, String.class, String.class, String.class, String.class, String.class);

	
	//LETHAL ACCIDENT PER CONTRIBUTING FACTOR
	final DataSet<Tuple6<Integer,String,String,String,String,String>> cf_fields = csvData
	    .project(2,3,4,5,6,7);
	final DataSet<Tuple7<Integer,Integer,String,String,String,String,String>> cf_lethal = cf_fields
	    .flatMap(setLethal);
	final DataSet<Tuple3<Integer,String,Integer>> cf_list_complete = cf_lethal
	    .reduceGroup(reduceFunc);
	final DataSet<Tuple3<Integer,String,Integer>> cf_list_complete_filtered = cf_list_complete
	    .filter(cfFilter);
	final DataSet<Tuple2<Integer,String>> accidentCount = cf_list_complete_filtered
	    .project(0,1);
	final DataSet<Tuple2<Integer,String>> accidentCounter_summed = accidentCount
	    .groupBy(1)
	    .sum(0);
	final DataSet<Tuple2<Integer,String>> lethalCount = cf_list_complete_filtered
	    .project(2,1);
	final DataSet<Tuple2<Integer,String>> lethalCounter_summed = lethalCount
	    .groupBy(1)
	    .sum(0);
	final DataSet<Tuple3<Integer,String,Integer>> cf_stats_pretreatment = accidentCounter_summed
	    .join(lethalCounter_summed)
	    .where(1)
	    .equalTo(1)
	    .projectFirst(0)
	    .projectSecond(1,0);
	final DataSet<Tuple4<Integer,String,Integer,String>> cf_stats = cf_stats_pretreatment
	    .reduceGroup(reducePercentage);
	
	//LETHAL ACCIDENT PER WEEK
	final DataSet<Tuple2<String,Integer>> dateOfAccidents = csvData
	    .project(0,2);
	final DataSet<Tuple2<String,Integer>> filtered_dateOfAccidents = dateOfAccidents
	    .filter(lethalFilter);
	final DataSet<Tuple2<String,Integer>> counter_dateOfAccidents = dateOfAccidents
	    .flatMap(lethalCounter);
	final DataSet<Tuple2<String,Integer>> accidentPerWeek = counter_dateOfAccidents
	    .groupBy(0)
	    .sum(1);
	
	//NUMBER OF ACCIDENT AND AVERAGE NUMBER OF LETHAL ACCIDENT PER WEEK PER BOROUGH (Quite messy without the flink table API)
	final DataSet<Tuple3<String,String,Integer>> accidentLocationDateDeath = csvData
	    .project(0,1,2);
	final DataSet<Tuple4<Integer,String,String,Integer>> accidentLocationWeekLethal = accidentLocationDateDeath
	    .flatMap(lethalCounter2);
	
	/*here's the tricky part now since the flink's DataSet API doesn't provide the possibilty of summing two fields at once*/

	final DataSet<Tuple3<Integer,String,String>> split1 = accidentLocationWeekLethal.project(0,1,2);
	final DataSet<Tuple3<Integer,String,String>> summed_split1 = split1
	    .groupBy(1,2)
	    .sum(0); //Get the number of accident for each week per burough
	final DataSet<Tuple2<String,Integer>> sub_split1 = summed_split1
	    .project(2,0);
	final DataSet<Tuple2<String,Integer>> sub_summed_split1 = sub_split1
	    .groupBy(0)
	    .sum(1);//Number of accident IN TOTAL per borough
	
	final DataSet<Tuple3<String,String,Integer>> split2 = accidentLocationWeekLethal.project(1,2,3);
	final DataSet<Tuple3<String,String,Integer>> summed_split2 = split2
	    .groupBy(0,1)
	    .sum(2); //Get the number of lethal accident for each week per borough
	final DataSet<Tuple2<String,Integer>> sub_split2 = summed_split2
	    .project(1,2);
	final DataSet<Tuple2<String,Integer>> sub_summed_split2 = sub_split2
	    .groupBy(0)
	    .sum(1); //Number of lethal accident IN TOTAL per borough
	/*Now that we have the number of accident and lethal accident for each week per borough, we want to compute the avg of both of them PER WEEK*/
	/*We need to know the number of week per burough we have*/
	final DataSet<Tuple1<String>> weekSplit = summed_split1.project(2); //Works also with summed_split2
	final DataSet<Tuple2<String,Integer>> summed_weekSplit = weekSplit
	    .flatMap(weekCounter)
	    .groupBy(0)
	    .sum(1);
	/*Finally we join it all together*/
	final DataSet<Tuple3<Integer,Integer,String>> accidentPerBoroughInTotal = sub_summed_split1
	    .join(sub_summed_split2)
	    .where(0)
	    .equalTo(0)
	    .projectFirst(1)
	    .projectSecond(1,0);
	/*We add how many week we have per borough*/
	final DataSet<Tuple4<Integer,Integer,String,Integer>> week_and_accidentPerBoroughInTotal = accidentPerBoroughInTotal
	    .join(summed_weekSplit)
	    .where(2)
	    .equalTo(0)
	    .projectFirst(0,1,2)
	    .projectSecond(1);
	/*Let's reduceGroup it and we're done*/
	final DataSet<Tuple4<Integer,Integer,String,String>> avgAccidentPerWeekPerBorough = week_and_accidentPerBoroughInTotal.reduceGroup(reduceAvgPerWeek);

	
	/*Print of the output*/
	accidentPerWeek.writeAsCsv("file:/home/antking/Bureau/accidentPerWeek.csv", "\n", ",");
	cf_stats.writeAsCsv("file:/home/antking/Bureau/cf_stats.csv", "\n", ",");
        avgAccidentPerWeekPerBorough.writeAsCsv("file:/home/antking/Bureau/avgAccidentPerWeekPerBorough.csv", "\n", ",");
	env.execute();
    } 


    /*\******************************************\*/
    /*               FUNCTIONS                    */
    /*\******************************************\*/


    //Return the date with a proper format for our tuples
    public static String getCurrentWeek(String input) throws Exception {
	String format = "LL/dd/yyyy";
	try {
	    SimpleDateFormat df = new SimpleDateFormat(format);
	    Date date = df.parse(input);
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    return "Week "+ cal.get(Calendar.WEEK_OF_YEAR)+" of "+ (2000+(date.getYear()-100));
	} catch (ParseException e) {}
	return "undefined";
    }
    
    //lethalFilter: keep the lethal accidents only
    private static final FilterFunction<Tuple2<String,Integer>> lethalFilter = new FilterFunction<Tuple2<String,Integer>>() {
	@Override
	    public boolean filter(Tuple2<String,Integer> in)throws Exception{
		final int nb_person_killed = in.f1;
		return nb_person_killed > 0;
	}
    };
    
    //cfFilter: Remove bad fields of contributing factor
    private static final FilterFunction<Tuple3<Integer,String,Integer>> cfFilter = new FilterFunction<Tuple3<Integer,String,Integer>>() {
	@Override
	    public boolean filter(Tuple3<Integer,String,Integer> in) throws Exception{
		final String contributing_factor = in.f1;
		return ((!contributing_factor.contains("0"))&&(!contributing_factor.contains("1"))&&(!contributing_factor.contains("2"))&&(!contributing_factor.contains("3"))&&(!contributing_factor.contains("4"))&&(!contributing_factor.contains("5")));
	}
    };

    //lethalCounter : compute the week and return a tuple with the week and a counter field (e.g ("Week 20 of 2016",1))
    private static final FlatMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>> lethalCounter = new FlatMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>() {
	    @Override
	    public void flatMap(Tuple2<String,Integer> in, Collector<Tuple2<String,Integer>> out) throws Exception{
		String date = getCurrentWeek(in.f0);
		out.collect(new Tuple2<String,Integer>(date,1));
	    }
	};
    

    //lethalCounter2 : returns the accident counter + right date format + borough + lethal counter 
    private static final FlatMapFunction<Tuple3<String,String,Integer>,Tuple4<Integer,String,String,Integer>> lethalCounter2 = new FlatMapFunction<Tuple3<String,String,Integer>,Tuple4<Integer,String,String,Integer>>() {
	    @Override
	    public void flatMap(Tuple3<String,String,Integer> in, Collector<Tuple4<Integer,String,String,Integer>> out) throws Exception{
		String date = getCurrentWeek(in.f0);
		String borough = in.f1;
		Integer lethal = in.f2 > 0 ? 1 : 0;
		out.collect(new Tuple4<Integer,String,String,Integer>(1,date,borough,lethal));
	    }
	};

    private static final FlatMapFunction<Tuple1<String>,Tuple2<String,Integer>> weekCounter = new FlatMapFunction<Tuple1<String>,Tuple2<String,Integer>>() {
	    @Override
	    public void flatMap(Tuple1<String> in, Collector<Tuple2<String,Integer>> out) throws Exception{
		out.collect(new Tuple2<String,Integer>(in.f0,1));
	    }
	};

    //setLethal : add a field to the tuple that indicate whether the accident is lethal or not.
    //Also add a counter field (f0)
    private static final FlatMapFunction<Tuple6<Integer,String,String,String,String,String>, Tuple7<Integer,Integer,String,String,String,String,String>> setLethal = new FlatMapFunction<Tuple6<Integer,String,String,String,String,String>, Tuple7<Integer,Integer,String,String,String,String,String>>() {
	    @Override
	    public void flatMap(Tuple6<Integer,String,String,String,String,String> in, Collector<Tuple7<Integer,Integer,String,String,String,String,String>> out) throws Exception{

		final int nb_person_killed = in.f0;
		final String cf_f1 = in.f1;
		final String cf_f2 = in.f2;
		final String cf_f3 = in.f3;
		final String cf_f4 = in.f4;
		final String cf_f5 = in.f5;
		
		if (nb_person_killed>0)
		    out.collect(new Tuple7<Integer,Integer,String,String,String,String,String>(1,1,cf_f1,cf_f2,cf_f3,cf_f4,cf_f5)); //Lethal
		else
		    out.collect(new Tuple7<Integer,Integer,String,String,String,String,String>(1,0,cf_f1,cf_f2,cf_f3,cf_f4,cf_f5)); //Non-Lethal
	    }
	};
    
    // reduceFunc : The goal here is to avoid duplicata (e.g (1,1,Illness,Glare,Illness,,) returns (1,Illness,1) + (1,Glare,1)  and no extra (1,Illness,1) that could jeopardize the results
    private static final GroupReduceFunction<Tuple7<Integer,Integer,String,String,String,String,String>,Tuple3<Integer,String,Integer>> reduceFunc = new GroupReduceFunction< Tuple7<Integer,Integer,String,String,String,String,String>,Tuple3<Integer,String,Integer> >(){
    	    @Override
    	    public void reduce(Iterable<Tuple7<Integer,Integer,String,String,String,String,String>> in, Collector<Tuple3<Integer,String,Integer>> out)
    	    {
    		for (Tuple7<Integer,Integer,String,String,String,String,String> tuple : in)
    		    {
			ArrayList<String> dup = new ArrayList<String>();
    			Tuple3<Integer,String,Integer> t1 = new Tuple3<Integer,String,Integer>(tuple.f0,tuple.f2,tuple.f1);
    			Tuple3<Integer,String,Integer> t2 = new Tuple3<Integer,String,Integer>(tuple.f0,tuple.f3,tuple.f1);
    			Tuple3<Integer,String,Integer> t3 = new Tuple3<Integer,String,Integer>(tuple.f0,tuple.f4,tuple.f1);
    			Tuple3<Integer,String,Integer> t4 = new Tuple3<Integer,String,Integer>(tuple.f0,tuple.f5,tuple.f1);
    			Tuple3<Integer,String,Integer> t5 = new Tuple3<Integer,String,Integer>(tuple.f0,tuple.f6,tuple.f1);
			
			if (!dup.contains(tuple.f2))
			    {
				dup.add(tuple.f2);
				out.collect(t1);
			    }
			if (!dup.contains(tuple.f3))
			    {
				dup.add(tuple.f3);
				out.collect(t2);
			    }
			if (!dup.contains(tuple.f4))
			    {
				dup.add(tuple.f4);
				out.collect(t3);
			    }
			if (!dup.contains(tuple.f5))
			    {
				dup.add(tuple.f5);
				out.collect(t4);
			    }
			if (!dup.contains(tuple.f6))
			    {
				dup.add(tuple.f6);
				out.collect(t5);
			    }
			//flush
			dup.clear();
    		    }
    	    }
    	};
    
    //reducePercentage : Tuple3->Tuple4 compute the percentage of lethal accident for each contributing factor
    private static final GroupReduceFunction<Tuple3<Integer,String,Integer>,Tuple4<Integer,String,Integer,String>> reducePercentage = new GroupReduceFunction<Tuple3<Integer,String,Integer>,Tuple4<Integer,String,Integer,String>>(){
    	    @Override
    	    public void reduce(Iterable<Tuple3<Integer,String,Integer>> in, Collector<Tuple4<Integer,String,Integer,String>> out) throws Exception
    	    {
		String percentage_field = null;

		for (Tuple3<Integer,String,Integer> tuple : in)
		    {
			int i1 = tuple.f2;
			int i2 = tuple.f0;
			double d = 0;
			try{
			    d = ((double)i1/(double)i2)*100.0;
			} catch (ArithmeticException e){}
			percentage_field =  Double.toString(d) + " %";
			out.collect(new Tuple4<Integer,String,Integer,String>(tuple.f0,tuple.f1,tuple.f2,percentage_field));
		    }
	    }
	};
    
    //reduceAvgPerWeek: Returns the number of accident and lethal accident per week per borough
    private static final GroupReduceFunction<Tuple4<Integer,Integer,String,Integer>,Tuple4<Integer,Integer,String,String>> reduceAvgPerWeek = new GroupReduceFunction<Tuple4<Integer,Integer,String,Integer>,Tuple4<Integer,Integer,String,String>>(){
    	    @Override
    	    public void reduce(Iterable<Tuple4<Integer,Integer,String,Integer>> in, Collector<Tuple4<Integer,Integer,String,String>> out) throws Exception
    	    {
		for(Tuple4<Integer,Integer,String,Integer> tuple : in)
		    {
			int numberOfAccident= tuple.f0;
			int numberOfLethalAccident = tuple.f1;
			String borough = tuple.f2;
			int numberOfWeek = tuple.f3;
			double avg = 0;
			try{
			    avg = (double)numberOfLethalAccident/(double)numberOfWeek;
			} catch (ArithmeticException e){}
			out.collect(new Tuple4<Integer,Integer,String,String> (numberOfAccident,numberOfLethalAccident,borough,"Avg : "+avg));
		    }
	    }
	}; 
}
