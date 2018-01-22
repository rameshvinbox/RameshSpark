package com.rd

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.InputStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
import grizzled.slf4j.Logging


object SampleSpark extends Logging {
	def main(args: Array[String]) {
	   info("Starting...")
	   System.setProperty("hadoop.home.dir", "C:\\Users\\pb492\\Documents\\Hadoop");
		val spark = SparkSession.builder
		            .master("local")
		            .appName("SampleSpark")
		            .getOrCreate()

			
		//Read some example file to a test RDD
    val test = spark.sparkContext.textFile("input.txt")

    val configuration = new Configuration();
    val fs = FileSystem.get(configuration);
    fs.delete(new Path("output.txt"), true) // delete file, true for recursive 

    test.flatMap { line => //for each line
      line.split(" ") //split the line in word by word.
    }
      .map { word => //for each word
        (word, 1) //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .saveAsTextFile("output.txt") //Save to a text file

    
    println("Completed!!")
    info("Finished...")   
    
    
    

		spark.stop()
	}
}
// scalastyle:on println
