package edu.cuhk.cse.spark.examples;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public final class BasicCount {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: JavaWordCount <file> <number of partitions> <strong level>");
			System.exit(1);
		}

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		StorageLevel level = null;
		if ( args[ 2 ].equals( "MEMORY_ONLY" ) )
			level = StorageLevel.MEMORY_ONLY();
		else if ( args[ 2 ].equals( "MEMORY_AND_DISK" ) )
			level = StorageLevel.MEMORY_AND_DISK();
		else if ( args[ 2 ].equals( "MEMORY_ONLY_SER" ) )
			level = StorageLevel.MEMORY_ONLY_SER();
		else if ( args[ 2 ].equals( "MEMORY_AND_DISK_SER" ) )
			level = StorageLevel.MEMORY_AND_DISK_SER();
		else if ( args[ 2 ].equals( "DISK_ONLY" ) )
			level = StorageLevel.DISK_ONLY();
		else if ( args[ 2 ].equals( "MEMORY_ONLY_2" ) )
			level = StorageLevel.MEMORY_ONLY_2();
		else if ( args[ 2 ].equals( "MEMORY_AND_DISK_2" ) )
			level = StorageLevel.MEMORY_AND_DISK_2();
		else if ( args[ 2 ].equals( "MEMORY_ONLY_SER_2" ) )
			level = StorageLevel.MEMORY_ONLY_SER_2();
		else if ( args[ 2 ].equals( "MEMORY_AND_DISK_SER_2" ) )
			level = StorageLevel.MEMORY_AND_DISK_SER_2();
		else if ( args[ 2 ].equals( "DISK_ONLY_2" ) )
			level = StorageLevel.DISK_ONLY_2();
		else if ( args[ 2 ].equals( "OFF_HEAP" ) )
			level = StorageLevel.OFF_HEAP();
		else {
			System.err.println( "Unknown storage level. Supported storage level: MEMORY_ONLY / MEMORY_AND_DISK / MEMORY_ONLY_SER / MEMORY_AND_DISK_SER / DISK_ONLY / OFF_HEAP" );
			System.exit( 1 );
		}

		SparkConf sparkConf = new SparkConf().setAppName("BasicCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], Integer.parseInt(args[1]));
		
		lines.persist(level);
		lines.count();
		
		ctx.stop();
	}
}
