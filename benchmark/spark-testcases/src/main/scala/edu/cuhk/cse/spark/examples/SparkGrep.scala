package edu.cuhk.cse.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkGrep {
	def main(args: Array[String]) {
		if (args.length < 4) {
			System.err.println("Usage: SparkGrep <input_file> <number of partitions> <storage level> <match_term>")
			System.exit(1)
		}
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)

		var level: StorageLevel = null
		if ( args( 2 ).equals( "MEMORY_ONLY" ) )
			level = StorageLevel.MEMORY_ONLY
		else if ( args( 2 ).equals( "MEMORY_AND_DISK" ) )
			level = StorageLevel.MEMORY_AND_DISK
		else if ( args( 2 ).equals( "MEMORY_ONLY_SER" ) )
			level = StorageLevel.MEMORY_ONLY_SER
		else if ( args( 2 ).equals( "MEMORY_AND_DISK_SER" ) )
			level = StorageLevel.MEMORY_AND_DISK_SER
		else if ( args( 2 ).equals( "DISK_ONLY" ) )
			level = StorageLevel.DISK_ONLY
		else if ( args( 2 ).equals( "MEMORY_ONLY_2" ) )
			level = StorageLevel.MEMORY_ONLY_2
		else if ( args( 2 ).equals( "MEMORY_AND_DISK_2" ) )
			level = StorageLevel.MEMORY_AND_DISK_2
		else if ( args( 2 ).equals( "MEMORY_ONLY_SER_2" ) )
			level = StorageLevel.MEMORY_ONLY_SER_2
		else if ( args( 2 ).equals( "MEMORY_AND_DISK_SER_2" ) )
			level = StorageLevel.MEMORY_AND_DISK_SER_2
		else if ( args( 2 ).equals( "DISK_ONLY_2" ) )
			level = StorageLevel.DISK_ONLY_2
		else if ( args( 2 ).equals( "OFF_HEAP" ) )
			level = StorageLevel.OFF_HEAP
		else {
			println( "Unknown storage level. Supported storage level: MEMORY_ONLY / MEMORY_AND_DISK / MEMORY_ONLY_SER / MEMORY_AND_DISK_SER / DISK_ONLY / OFF_HEAP" )
			System.exit( 1 )
		}

		val conf = new SparkConf().setAppName("SparkGrep")
		val sc = new SparkContext(conf)
		val inputFile = sc.textFile(args(0), args(1).toInt).persist( level )
		val matchTerm : String = args(3)
		val numMatches = inputFile.filter(line => line.contains(matchTerm)).count()
		println("%s lines in %s contain %s".format(numMatches, args(0), matchTerm))
		System.exit(0)
	}
}
