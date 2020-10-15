package DataComparision.BeyondCompare;

import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class DataComparision {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local").appName("BeyondCompare").getOrCreate();
		//Input directory location for File1 to be compared
		String inputLocation1 = args[0];
		//Input Directory location for File2 to be compared
		String inputLocation2 = args[1];
		//Output location to dump the results
		String outputLocation = args[2];
		//Properties file location
		String propertiesFileLocation = args[3];
		OutputValidator.Validate(spark,inputLocation1,inputLocation2,outputLocation,propertiesFileLocation);

	}
}
