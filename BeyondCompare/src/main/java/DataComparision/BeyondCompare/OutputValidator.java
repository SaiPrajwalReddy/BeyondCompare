package DataComparision.BeyondCompare;

import java.io.File;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Wrapper class used to read data from the files and order them on the basis of primary key
 * 
 */
public class OutputValidator {

	public static void Validate(SparkSession spark, String inputLocationForFile1, String inputLocationForFile2,
			String outputLocation, String propertiesFileLocation) {

		// Read the properties file
		Dataset<Row> propertiesDF = spark.read().format("csv").option("header", "true").option("inferschema", "true")
				.option("null", "").option("delimiter", "~").option("timestampFormat", "yyyy-MM-dd")
				.option("mode", "DROPMALFORMED").option("path", propertiesFileLocation + "/properties").load();

		List<Row> propertiesList = propertiesDF.collectAsList();
		for (Row row : propertiesList) {
			String primaryKey = row.getString(0);
			String[] primaryKeys = primaryKey.split(",");
			Column[] primary = new Column[primaryKeys.length];
			int i = 0;
			for (String key : primaryKeys) {
				primary[i++] = new Column(key);
			}
			String outputFileName = row.getString(1);
			String delimiter = row.getString(2);
			Boolean isHeaderPresent = row.getBoolean(3);

			// Read file1
			Dataset<Row> File1 = spark.read().format("csv").option("header", isHeaderPresent.toString())
					.option("inferschema", "true").option("null", "").option("delimiter", delimiter)
					.option("timestampFormat", "yyyy-MM-dd").option("mode", "DROPMALFORMED")
					.option("path", inputLocationForFile1 + File.separator + outputFileName + "/part-*").load();

			// Read file2
			Dataset<Row> File2 = spark.read().format("csv").option("header", isHeaderPresent.toString())
					.option("inferschema", "true").option("null", "").option("delimiter", delimiter)
					.option("timestampFormat", "yyyy-MM-dd").option("mode", "DROPMALFORMED")
					.option("path", inputLocationForFile2 + File.separator + outputFileName + "/part-*").load();

			Dataset<Row> File1OrderedByPkey = null;
			Dataset<Row> File2OrderedByPkey = null;

			// Order the data on primary key to compare outputs
			File1OrderedByPkey = File1.orderBy(primary);
			File2OrderedByPkey = File2.orderBy(primary);

			CompareOutputs.generateRDD(File1OrderedByPkey, File2OrderedByPkey, outputFileName, spark, primaryKey,
					outputLocation);
		}

	}
}
