package DataComparision.BeyondCompare;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;
/*
 * Class used to generate RDDs from Dataframes and compare the results. 
 * */
public class CompareOutputs {
	static StructField[] fields = null;
	static List<String> mismatchesList = new ArrayList<String>();
	static List<String> fieldsDataType = new ArrayList<String>();
	static String primaryKey;

	public static void generateRDD(Dataset<Row> File1, Dataset<Row> File2, String fileName,
			SparkSession spark, String primaryKey, String outputLocation) {
		
		CompareOutputs.primaryKey = primaryKey;
		String dataType = null;
		fields = File1.schema().fields();
		for (StructField field : fields) {

			dataType = field.dataType().typeName();
			fieldsDataType.add(dataType);

		}
		Long File1RecCount = File1.count();
		Long File2RecCount = File2.count();
		
		// Check for record count in both the files		
		if (Long.compare(File1RecCount, File2RecCount) == 0) {
			
			// Assign indexes to reach row
			JavaPairRDD<Row, Long> File1javaRDD = File1.javaRDD().zipWithIndex();
			JavaPairRDD<Row, Long> File2javaRDD = File2.javaRDD().zipWithIndex();

			JavaRDD<Tuple2<Long, Row>> File1javaRDDresult = File1javaRDD
					.map(new Function<Tuple2<Row, Long>, Tuple2<Long, Row>>() {

						/**
						 *  InterChange column1 data with column2
						 */
						
						private static final long serialVersionUID = 1L;

						public Tuple2<Long, Row> call(Tuple2<Row, Long> v1) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Long, Row>(v1._2, v1._1);
						}
					});
			JavaPairRDD<Long, Row> File1FinalPairRdd = File1javaRDDresult
					.mapToPair(new PairFunction<Tuple2<Long, Row>, Long, Row>() {
						
						/**
						 *  Generate RDD pairs 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Long, Row> call(Tuple2<Long, Row> t) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Long, Row>(t._1, t._2);
						}

					});

			JavaRDD<Tuple2<Long, Row>> File2javaRDDresult = File2javaRDD
					.map(new Function<Tuple2<Row, Long>, Tuple2<Long, Row>>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Long, Row> call(Tuple2<Row, Long> v1) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Long, Row>(v1._2, v1._1);
						}

					});
			JavaPairRDD<Long, Row> File2FinalPairRdd = File2javaRDDresult
					.mapToPair(new PairFunction<Tuple2<Long, Row>, Long, Row>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Long, Row> call(Tuple2<Long, Row> t) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Long, Row>(t._1, t._2);
						}

					});
			
			JavaPairRDD<Long, Row> cacheFile1javaRDD = File1FinalPairRdd.cache();
			JavaPairRDD<Long, Row> cacheFile2javaRDD = File2FinalPairRdd.cache();
			
			//Perform a Join Operation on the zipped index. Iterate every row and compare 
			cacheFile1javaRDD.join(cacheFile2javaRDD)
					.foreach(new VoidFunction<Tuple2<Long, Tuple2<Row, Row>>>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 4045836526151812037L;

						@Override
						public void call(Tuple2<Long, Tuple2<Row, Row>> v1) throws Exception {
							// TODO Auto-generated method stub
							CompareOutputs.compare(v1._2._1, v1._2._2);
						}
					});
			;
			
			// Collect the mismatches in a list and convert to dataframe to dump output
			if (null != mismatchesList) {
				if (mismatchesList.size() != 0) {
					Dataset<String> resultDf = spark.createDataset(mismatchesList, Encoders.STRING());
					resultDf.show();
					resultDf.write().format("com.databricks.spark.csv").option("delimiter", "|")
							.option("timestampFormat", "yyyy-MM-dd").save(outputLocation + fileName);
				} else {
					System.out.println("Results are perfectly matching");
				}
			}

		} else

		{
			// If there is a mismatch in record count dump the uncommon records not present in both the files
			
			File1.createOrReplaceTempView("File1");
			File2.createOrReplaceTempView("File2");
			String primaryKeys[] = primaryKey.split(",");
			StringBuffer sb = new StringBuffer();
			StringBuffer sb1 = new StringBuffer();
			for (int i = 0; i < primaryKeys.length; i++) {
				sb.append(primaryKeys[i]).append(",");
				sb1.append("File1.").append(primaryKeys[i]).append("=").append("File2.")
						.append(primaryKeys[i]).append(" AND ");
			}
			String selectClause = sb.substring(0, sb.length() - 1);
			String whereClause = sb1.substring(0, sb1.lastIndexOf("AND")).concat(")");
			String query1 = null;
			String query2 = null;
			if (File1.count() > File2.count()) {
				query1 = "SELECT " + selectClause
						+ " FROM File1 WHERE NOT EXISTS ( SELECT * FROM File2 WHERE "
						+ whereClause;
				System.out.println("Result for Count misMatch is ");
				spark.sql(query1).show();

				spark.sql(query1).write().format("com.databricks.spark.csv").option("delimiter", "|")
						.option("timestampFormat", "yyyy-MM-dd")
						.save(outputLocation + fileName + "_notFoundRecordsInFile2");

				query2 = "SELECT " + selectClause
						+ " FROM File2 WHERE NOT EXISTS ( SELECT * FROM File1 WHERE "
						+ whereClause;
				spark.sql(query2).show();

				spark.sql(query2).write().format("com.databricks.spark.csv").option("delimiter", "|")
						.option("timestampFormat", "yyyy-MM-dd")
						.save(outputLocation + fileName + "_notFoundRecordsInFile1");
			} else {
				query1 = "SELECT " + selectClause
						+ " FROM File2 WHERE NOT EXISTS ( SELECT * FROM File1 WHERE "
						+ whereClause;
				spark.sql(query1).show();

				spark.sql(query1).write().format("com.databricks.spark.csv").option("delimiter", "|")
						.option("timestampFormat", "yyyy-MM-dd")
						.save(outputLocation + fileName + "_notFoundRecordsInFile1");

				query2 = "SELECT " + selectClause
						+ " FROM File1 WHERE NOT EXISTS ( SELECT * FROM File2 WHERE "
						+ whereClause;
				System.out.println("Result for Count misMatch is ");
				spark.sql(query2).show();

				spark.sql(query2).write().format("com.databricks.spark.csv").option("delimiter", "|")
						.option("timestampFormat", "yyyy-MM-dd")
						.save(outputLocation + fileName + "_notFoundRecordsInFile2");
			}

		}		
		mismatchesList.clear();
		fieldsDataType.clear();

	}

	public static void compare(Row File1Row, Row File2Row) {
		// TODO Auto-generated method stub
		Long rowIndex = 0L;
		rowIndex++;
		int len = File1Row.length();
		int idx = 0;
		String primaryKeys[] = primaryKey.split(",");
		StringBuffer primaryKeyVal = new StringBuffer();
		for (int i = 0; i < primaryKeys.length; i++) {

			primaryKeyVal.append(File1Row.getAs(primaryKeys[i])).append('|');
		}
		String primaryKeyFinal = primaryKeyVal.substring(0, primaryKeyVal.length() - 1);
		
		//Compare column wise
		while (idx < len) {
			String mismatch = null;
			String type = fieldsDataType.get(idx);
			String columnName = fields[idx].name();

			if (type.equalsIgnoreCase("Double")) {
				
				// To compare upto 2 decimal places
				DecimalFormat f = new DecimalFormat("0.00");
				Double File2Val = null;
				Double File1Val = null;
				if (!File1Row.isNullAt(idx)) {
					File1Val = Double.parseDouble(f.format(File1Row.getDouble(idx)));
				}
				if (!File2Row.isNullAt(idx))
					File2Val = Double.parseDouble(f.format(File2Row.getDouble(idx)));

				if (File1Val != null && File2Val != null) {
					if (Double.parseDouble(f.format(Math.abs(File1Val - File2Val))) > 0.01) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;
					}
				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;

				}
			} else if (type.equalsIgnoreCase("timeStamp")) {

				String File1Val = null;
				String File2Val = null;
				if (null != File1Row.getTimestamp(idx))
					File1Val = File1Row.getTimestamp(idx).toString().substring(0, 10);
				if (null != File2Row.getTimestamp(idx))
					File2Val = File2Row.getTimestamp(idx).toString().substring(0, 10);
				if (File1Val != null && File2Val != null) {
																				
					if (!File2Val.equals(File1Val)) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~"
								+ File1Val + "~" + File2Val;
					}

				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;

				}

			} else if (type.equalsIgnoreCase("Integer")) {

				Integer File1Val = null;
				Integer File2Val = null;
				if (!File1Row.isNullAt(idx))
					File1Val = File1Row.getInt(idx);
				if (!File2Row.isNullAt(idx))
					File2Val = File2Row.getInt(idx);
				if (File1Val != null && File2Val != null) {
					if (Integer.compare(File1Val, File2Val) != 0) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;
					}
				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;

				}

			} else if (type.equalsIgnoreCase("Long")) {
				Long File1Val = null;
				Long File2Val = null;
				if (!File1Row.isNullAt(idx))
					File1Val = File1Row.getLong(idx);
				if (!File2Row.isNullAt(idx))
					File2Val = File2Row.getLong(idx);

				if (File1Val != null && File2Val != null) {
					if (Long.compare(File1Val, File2Val) != 0) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;
					}
				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;

				}
			} else if (type.equalsIgnoreCase("Boolean")) {
				Boolean File1Val = null;
				Boolean File2Val = null;
				if (!File1Row.isNullAt(idx))
					File1Val = File1Row.getBoolean(idx);
				if (!File2Row.isNullAt(idx))
					File2Val = File2Row.getBoolean(idx);
				if (File1Val != null && File2Val != null) {
					if (Boolean.compare(File1Val, File2Val) != 0) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;
					}
				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;

				}
			} else if (type.equalsIgnoreCase("String")) {

				String File1Val = null;
				String File2Val = null;
				if (!File1Row.isNullAt(idx))
					File1Val = File1Row.getString(idx);
				if (!File2Row.isNullAt(idx))
					File2Val = File2Row.getString(idx);
				if (File1Val != null && File2Val != null) {
					if (!File1Val.equalsIgnoreCase(File2Val)) {
						mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
								+ File1Val;
					}
				} else {
					if ((File1Val != null && File2Val == null) || (File1Val == null && File2Val != null))
						if (!((File1Val != null && File1Val.equalsIgnoreCase("NULL"))
								|| (File2Val != null && File2Val.equalsIgnoreCase("NULL"))))
							mismatch = columnName + "~" + primaryKeyFinal + "~" + rowIndex + "~" + File2Val + "~"
									+ File1Val;

				}
			}
			idx++;
			if (null != mismatch)
				mismatchesList.add(mismatch);

		}

	}

}




