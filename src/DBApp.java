import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.constant.Constable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.text.DateFormat;
import java.text.DecimalFormat;

public class DBApp {
	static Writer writer;
	static Reader reader;
	static Hashtable<String, Table> tables;
	static int maxTableSize;

	public static void init() {
		writer = new Writer();
		reader = new Reader();
		tables = new Hashtable<>();
		// deserializeAll();
		if (tables.isEmpty())
			createMetaDataHeader();

		Properties properties = new Properties();
		try (FileReader reader = new FileReader("DBApp.config")) {
			properties.load(reader);
			String value1 = properties.getProperty("MaximumRowsCountinTablePage");
			maxTableSize = Integer.parseInt(value1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createMetaDataHeader() {
		String csvFilePath = "metadata.csv";
		String headers = "TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,min,max,ForeignKey,ForeignTableName,ForeignColumnName,Computed";
		writer.overwriteFile(csvFilePath, headers);
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax, Hashtable<String, String> htblForeignKeys, String[] computedCols)
			throws DBAppException {
		Table t = new Table(strTableName, strClusteringKeyColumn);
		// serialize(t);
		String csvFilePath = "metadata.csv";
		ArrayList<String> arrayList = new ArrayList<>();
		Iterator<Map.Entry<String, String>> iterator = htblColNameType.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			StringBuilder row = new StringBuilder();
			row.append(strTableName);
			row.append(",");
			String colName = entry.getKey();
			row.append(colName);
			row.append(",");
			String colType = entry.getValue();
			row.append(colType);
			row.append(",");
			row.append(strClusteringKeyColumn.equals(colName) ? "True" : "False");
			row.append(",");
			row.append("null");
			row.append(",");
			row.append("null");
			row.append(",");
			String colMin = htblColNameMin.get(colName);
			row.append(colMin);
			row.append(",");
			String colMax = htblColNameMax.get(colName);
			row.append(colMax);
			row.append(",");
			String foreign;
			String[] foreignTableCol;
			String tableName = null;
			String tableCol = null;
			if (htblForeignKeys.containsKey(colName)) {
				foreign = htblForeignKeys.get(colName);
				foreignTableCol = foreign.split("\\.");
				tableName = foreignTableCol[0];
				tableCol = foreignTableCol[1];
			}
			row.append(tableName == null ? "False" : "True");
			row.append(",");
			row.append(tableName);
			row.append(",");
			row.append(tableCol);
			row.append(",");
			boolean computed = listContains(colName, computedCols);
			row.append(computed ? "True" : "False");
			arrayList.add(row.toString());
		}
		for (int i = 0; i < arrayList.size(); i++) {
			writer.appendToFile(csvFilePath, arrayList.get(i));
		}
		tables.put(strTableName, t);
	}

	public static boolean listContains(String s, String[] arr) {
		for (int i = 0; i < arr.length; i++) {
			if (s.equals(arr[i]))
				return true;
		}
		return false;
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		if (strarrColName.length != 2)
			throw new DBAppException("Two columns required to create an index");
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");

		Table t = tables.get(strTableName);
		String indName = strarrColName[0] + "_" + strarrColName[1] + "_index";

		// check if index already created

		String[][] metaData = reader.readCSV("metadata.csv");
		String[][] tableMeta = reader.readTableMeta(metaData, strTableName);
		String min1 = "", max1 = "", min2 = "", max2 = "", col1 = "", col2 = "";
		for (int i = 0; i < tableMeta.length; i++) {
			if (tableMeta[i][1].equals(strarrColName[0])) {
				min1 = tableMeta[i][6];
				max1 = tableMeta[i][7];
				col1 = tableMeta[i][2];
			} else if (tableMeta[i][1].equals(strarrColName[1])) {
				min2 = tableMeta[i][6];
				max2 = tableMeta[i][7];
				col2 = tableMeta[i][2];
			}
		}
		try {
			GridIndex grid = new GridIndex(indName, t, col1, strarrColName[0], min1, max1, col2, strarrColName[1], min2,
					max2);
			// serialize(grid);
			grid.getOnTable().getIndecies().add(grid);
			t.setHasIndex(true);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		try {
			Table curTable = tables.get(strTableName);
			// Get tuple to be inserted in Table and verify it with meta data
			String[] tupleInfo = getTuple(strTableName, htblColNameValue);
			String tuple = tupleInfo[0];
			String clusteringCol = tupleInfo[1];
			String clusteringType = tupleInfo[2];
			String clusteringVal = tupleInfo[3];
			int clusterColIndex = Integer.parseInt(tupleInfo[5]);
			Class colClass = Class.forName(clusteringType);
			Constructor constructor = colClass.getConstructor(String.class);
			Object clusterVal = constructor.newInstance(clusteringVal);
			int pageReq = -1;
			String[] header = tupleInfo[4].split(",");
			boolean indexOnCluster = false;
			GridIndex gridIndex = null;
			for (int i = 0; i < curTable.getIndecies().size(); i++) {
				GridIndex cur = curTable.getIndecies().get(i);
				if (cur.getName().contains(clusteringCol)) {
					indexOnCluster = true;
					gridIndex = cur;
					break;
				}
			}
			boolean allNull = true;
			indexLabel: {
				if (curTable.hasIndex() && indexOnCluster) {
					// if an index is created on the table on clustering key
					// using the gridIndex's get()
					// check which page the tuple should be inserted in using gridIndex.get()
					// get the page and insert into it the tuple
					String[] indexName = gridIndex.getName().split("_");
					String col1 = indexName[0];
					String col2 = indexName[1];

					String[] pageNames = gridIndex.getByCluster(clusteringVal, col1.equals(clusteringCol));
					ArrayList<String> pages = new ArrayList<>();
					for (int i = 0; i < pageNames.length; i++) {
						if (pageNames[i] == null) {
							continue;
						}
						String[] temp = pageNames[i].split("-");
						for (String s : temp)
							pages.add(s);
						allNull = false;
					}
					if (allNull) {
						break indexLabel;
					}
					int row = -1, rowReq = -1;
					int pageInPages = -1;
					for (int i = 0; i < pages.size(); i++) {

						String[][] page = reader.readCSV(curTable.getPath() + pages.get(i));
						for (row = 1; (row < page.length) && page[row][clusterColIndex] != null; row++) {
							// prepare the values to be compared
							// compare the values to find index of insertion
							Object curVal = constructor.newInstance(page[row][clusterColIndex]);
							if (((Comparable) clusterVal).compareTo(curVal) < 0) {
								String pageNum = pages.get(i).substring(4).replaceAll(Pattern.quote(".csv"), "");
								rowReq = row;
								pageInPages = i;
								pageReq = Integer.parseInt(pageNum) - 1;
								break;
							} else if (((Comparable) clusterVal).compareTo(curVal) == 0)
								throw new DBAppException(
										"Can not have duplicates of clustering key value: " + clusterVal);
						}
					}
					if (rowReq == -1) {
						String pageNum = pages.get(pages.size() - 1).substring(4).replaceAll(Pattern.quote(".csv"), "");
						rowReq = row;
						pageInPages = pages.size() - 1;
						pageReq = Integer.parseInt(pageNum) - 1;
					}
					String[][] page = reader
							.readNSizeTable(curTable.getPath() + File.separator + pages.get(pageInPages));
					String filePath = curTable.getPath() + File.separator + pages.get(pageInPages);
//					System.out.println(pageReq);
//					System.out.println(rowReq);
					if (rowReq > maxTableSize) {
						if (curTable.getPages().get(curTable.getPages().size() - 1).getName()
								.equals(pages.get(pageInPages))) {
							// it is the last page in the table
							// create a new page and insert the tuple
							filePath = curTable.getPath() + File.separator + "Page" + (curTable.getPages().size() + 1)
									+ ".csv";
							pageReq++;
							createPage(curTable, filePath);
							writer.appendToFile(filePath, tuple);
						} else {
							// add it to the next page then shift all tuples
							int t = 0;
							// find next page
							for (; t < curTable.getPages().size(); t++) {
								if (curTable.getPages().get(t).getName().equals(pages.get(pageInPages))) {
									t++;
									break;
								}
							}
							int pageNum = Integer.parseInt(
									pages.get(pageInPages).substring(4).replaceAll(Pattern.quote(".csv"), "")) + 1;
							page = shiftTuples(curTable, pageNum, rowReq);
							page[row] = tuple.split(",");
							writer.writePage(filePath, page);
						}
					} else if (page[rowReq][0] != null) {
						page = shiftTuples(curTable, pageReq, rowReq);
						page[row] = tuple.split(",");
						writer.writePage(filePath, page);
					} else {
						page[rowReq] = tuple.split(",");
						writer.writePage(filePath, page);
					}
				}
			}
			// No index is created on the table
			// so insert according to clustering key
			if (curTable.getPages().isEmpty() && allNull) {
				// Table is currently empty
				// create a new page and store the first tuple in it
				pageReq = 0;
				String filePath = curTable.getPath() + "Page" + (curTable.getPages().size() + 1) + ".csv";
				createPage(curTable, filePath);
				writer.appendToFile(filePath, tuple);
			} else if (allNull) {
				// Table already has Pages
				// iterate over existing pages/tuples to find location of current tuple
				// shift any other tuples that are below the current tuple
				int rowReq = -1;
				pageReq = -1;
				int row = 1;
				File firstPage = curTable.getPages().get(0);
				String filePath = firstPage.getPath();
				String[][] pageData = reader.readNSizeTable(filePath);
				for (int i = 0; i < curTable.getPages().size(); i++) {
					File curPage = curTable.getPages().get(i);
					pageData = reader.readNSizeTable(curPage.getPath());
					for (row = 1; (row < pageData.length) && pageData[row][clusterColIndex] != null; row++) {
						// prepare the values to be compared
						// compare the values to find index of insertion
						Object curVal = constructor.newInstance(pageData[row][clusterColIndex]);
						if (((Comparable) clusterVal).compareTo(curVal) < 0) {
							rowReq = row;
							pageReq = i;
							break;
						} else if (((Comparable) clusterVal).compareTo(curVal) == 0)
							throw new DBAppException(
									"Can not have duplicates of clustering key value: " + curVal.toString());
					}
				}
				if (rowReq == -1) {
					rowReq = row;
					pageReq = curTable.getPages().size() - 1;
				}
				// shift tuples if required
				String[][] page = reader.readNSizeTable(curTable.getPages().get(pageReq).getPath());
				if (rowReq >= maxTableSize + 1) {
					filePath = curTable.getPath() + "Page" + (curTable.getPages().size() + 1) + ".csv";
					pageReq++;
					createPage(curTable, filePath);
					writer.appendToFile(filePath, tuple);
				} else if (page[rowReq][0] != null) {
					page = shiftTuples(curTable, pageReq, rowReq);
					page[row] = tuple.split(",");
					filePath = curTable.getPages().get(pageReq).getPath();
					writer.writePage(filePath, page);
				} else {
					page[rowReq] = tuple.split(",");
					filePath = curTable.getPages().get(pageReq).getPath();
					writer.writePage(filePath, page);
				}
			}
			// insert pages into grid index
			for (int i = 0; i < curTable.getIndecies().size(); i++) {
				GridIndex cur = curTable.getIndecies().get(i);
				cur.insert(tuple.split(","), header, "Page" + (pageReq + 1));
			}
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| ClassNotFoundException | NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// shift tuples in the table for insertion method
	public static String[][] shiftTuples(Table table, int page, int row) {
		int i = table.getPages().size() - 1;
		int j = -1;
		for (; i >= page; i--) {
			String[][] pageContent = reader.readNSizeTable(table.getPages().get(i).getPath());
			for (j = pageContent.length - 1; j >= 1 && j != row; j--) {
				while (pageContent[j][0] == null) {
					j--;
				}
				if (j == 1 && i != table.getPages().size() - 1) {
					// if at the beginning of a file and need to read from previous file
					String[][] prevPage = reader.readNSizeTable(table.getPages().get(i - 1).getPath());
					String filePath = table.getPath() + "Page" + (table.getPages().size() + 1) + ".csv";
					pageContent[1] = prevPage[maxTableSize];
					writer.writePage(filePath, pageContent);
				} else if (j == maxTableSize && i == table.getPages().size() - 1) {
					// if at the last tuple of the last page
					// need to create a new page and shift into it
					String filePath = table.getPath() + "Page" + (table.getPages().size() + 1) + ".csv";
					createPage(table, filePath);
					writer.appendToFile(filePath, convertToString(pageContent[j]));
				} else if (j < maxTableSize) {
					// regular case
					// normal shifting
					while (j >= row) {
						pageContent[j + 1] = pageContent[j];
						j--;
					}
					return pageContent;
				}
			}
		}
		return null;
	}

	// create a new page for the table
	public static void createPage(Table table, String filePath) {
		File file = new File(filePath);
		try {
			if (file.createNewFile()) {
				// System.out.println("File created successfully.");
			} else {
				// System.out.println("File already exists.");
			}
		} catch (IOException e) {
			System.out.println("An error occurred while creating the file: " + e.getMessage());
		}
		table.getPages().add(file);
		StringBuilder colHeaders = new StringBuilder();
		String[][] metaData = reader.readCSV("metadata.csv");
		String[][] tableMeta = reader.readTableMeta(metaData, table.getName());
		for (int i = 0; i < tableMeta.length; i++) {
			colHeaders.append(tableMeta[i][1]);
			if (tableMeta.length - 1 != i)
				colHeaders.append(",");
		}
		writer.overwriteFile(filePath, colHeaders.toString());

	}

	// verify input data with metadata file
	// check min, max, foreign key, computed (if computed value is inserted throw an
	// error)
	// create tuple to be inserted
	public static String[] getTuple(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		String[] res = new String[6];
		StringBuilder sb = new StringBuilder();
		String[][] metaData;
		try {
			metaData = reader.readCSV("metadata.csv");
			String[][] tableMeta = reader.readTableMeta(metaData, strTableName);
			String[] header = new String[tableMeta.length];
			for (int i = 0; i < tableMeta.length; i++) {
				if (tableMeta[i][3].equals("True")) {
					res[1] = tableMeta[i][1];
					res[2] = tableMeta[i][2];
					res[5] = "" + i;
				}
				header[i] = tableMeta[i][1];
			}
			res[4] = convertToString(header);
			for (int i = 0; i < tableMeta.length; i++) {
				// getting column name and its value
				String colName = tableMeta[i][1];
				boolean computed = tableMeta[i][11].equals("True") ? true : false;
				Object value = null;
				if (!htblColNameValue.containsKey(colName) && computed) {
					// compute value
					value = (Double)Double.parseDouble(htblColNameValue.get("Quantity").toString());
					Table product = tables.get("Product");
					for (int a = 0; a < product.getPages().size(); a++) {
						String[][] ppage = reader.readCSV(product.getPages().get(a).getPath());
						String[] pheader = ppage[0];
						int cindex = 0;
						int pindex = 0;
						for (int b = 0; b < pheader.length; b++) {
							if (pheader[b].equals(product.getClusterCol()))
								cindex = b;
							if (pheader[b].equals("ProductPrice"))
								pindex = b;
						}
						boolean done = false;
						for (int b = 1; b < ppage.length; b++) {
							if (((int)htblColNameValue.get("ProductID")) == Integer.parseInt(ppage[b][cindex])) {
								value =Double.parseDouble(htblColNameValue.get("Quantity").toString()) * Double.parseDouble(ppage[b][pindex]);
								done = true;
								break;
							}
						}
						if (done)
							break;
					}
					String formattedNumber = String.format("%.2f", value);
					//System.out.println(formattedNumber);
					sb.append(formattedNumber);
				}
				else if (htblColNameValue.containsKey(colName) && computed)
					throw new DBAppException(colName + " is a computed col and should not be inserted");
				else 
					value = htblColNameValue.get(colName);
				if (colName.equals(res[1]))
					res[3] = value.toString();
				// getting control data from csv file to verify the input tuple
				String colType = tableMeta[i][2];
				String min = tableMeta[i][6];
				String max = tableMeta[i][7];
				String foreign = tableMeta[i][8];
				// check for foreign key and data type
				if (foreign.equals("True")) {
					String foreignTable = tableMeta[i][9];
					String foreignCol = tableMeta[i][10];
					if (!checkForForeignKey(foreignTable, foreignCol, value.toString()))
						throw new DBAppException(
								value.toString() + " Foreign Key does not exist in table " + foreignTable);
				}
				Class colClass;
				Constructor constructor;
				Object minObj;
				Object maxObj;
				if (colType.equals("java.lang.Date")) {
					colClass = Date.class;
					int[] minDate = getDate(min);
					int[] maxDate = getDate(max);
					minObj = new Date(minDate[2], minDate[1], minDate[0]);
					maxObj = new Date(maxDate[2], maxDate[1], maxDate[0]);
				} else {
					colClass = Class.forName(colType);
					constructor = colClass.getConstructor(String.class);
					minObj = constructor.newInstance(min);
					maxObj = constructor.newInstance(max);
				}
				if (!colClass.equals(value.getClass()))
					throw new DBAppException(colName + " should be of type " + colType);
				// comparing control data with input tuple
				if (((Comparable) value).compareTo(minObj) < 0)
					throw new DBAppException(colName + " value is less than the minimum value (" + min + ")");
				if (((Comparable) value).compareTo(maxObj) > 0)
					throw new DBAppException(colName + " value is greater than the maximum value (" + max + ")");
				if (colType.equals("java.lang.Date"))
					value = new String(convertDateFormat(value.toString()));
				if (!computed)
					sb.append(value.toString());
				if (i != tableMeta.length - 1)
					sb.append(",");
			}
			res[0] = sb.toString();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

	// changes date format from default to DD.MM.YYYY
	public static String convertDateFormat(String dateString) {
		DateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		DateFormat outputFormat = new SimpleDateFormat("dd.MM.yyyy");
		try {
			Date date = inputFormat.parse(dateString);
			String res = outputFormat.format(date);
			String[] arr = res.split("\\.");
			int year = Integer.parseInt(arr[2]) - 1900;
			int month = Integer.parseInt(arr[1]) - 1;
			arr[2] = "" + year;
			arr[1] = "" + month;
			StringBuilder sb = new StringBuilder();
			sb.append(arr[0]);
			sb.append(".");
			sb.append(arr[1]);
			sb.append(".");
			sb.append(arr[2]);
			return sb.toString();
		} catch (ParseException e) {
			e.printStackTrace();
			return null; // or throw an exception, depending on your use case
		}
	}

	// changes date format from DD.MM.YYYY to int[DD, MM, YYYY]
	public static int[] getDate(String s) {
		String[] date = s.split("\\.");
		int[] res = new int[3];
		res[0] = Integer.parseInt(date[0]);
		res[1] = Integer.parseInt(date[1]);
		res[2] = Integer.parseInt(date[2]);
		return res;
	}

	// checks if value is present in column colName in the table
	public static boolean checkForForeignKey(String table, String colName, String value) {
		Table t = tables.get(table);
//		if (t.hasIndex()) {
		// if no index is created
		// loop on all pages and compare the value with the tuples
		for (int i = 0; i < t.getPages().size(); i++) {
			String[][] page = reader.readCSV(t.getPages().get(i).getPath());
			String[] header = page[0];
			int col;
			for (col = 0; col < header.length; col++) {
				if (header[col].equals(colName)) {
					break;
				}
			}
			for (int j = 1; j < page.length; j++) {
				if (page[j][col].equals(value)) {
					return true;
				}
			}
		}
		return false;

	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// search for the tuple using its clustering value
		// update the values, then write the tuple back in the table
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		try {
			Table curTable = tables.get(strTableName);
			boolean exist = false;
			int gridindex = 0;
			for (int i = 0; i < curTable.getIndecies().size(); i++) {
				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String key = entry.getKey();
					if (curTable.getIndecies().get(i).getName().contains(curTable.getClusterCol())) {
						exist = true;
						gridindex = i;
					}
				}
			}
			// if there is an index created on the table required
			if (exist == true) {
				GridIndex cur = curTable.getIndecies().get(gridindex);
				String[] pages;
				ArrayList<String> afterSplit = new ArrayList<>();
				// get using cluster method and splitting
				String[] colNames = cur.getName().split("_");
				if (colNames[0].equals(strClusteringKeyValue))
					pages = cur.getByCluster(strClusteringKeyValue, true);
				else
					pages = cur.getByCluster(strClusteringKeyValue, false);
				for (int i = 0; i < pages.length; i++) {
					if (pages[i] == null)
						continue;
					String[] temp = pages[i].split("-");
					for (String s : temp)
						afterSplit.add(s);
				}
				HashSet<String> uniqueSet = new HashSet<>(afterSplit);
				afterSplit = new ArrayList<>(uniqueSet);
				for (int i = 0; i < afterSplit.size(); i++) {
					String[][] curpage = reader.readCSV(curTable.getPath() + afterSplit.get(i));
					String[][] headerAndTuple = findTupleUsingIndex(curTable, afterSplit, strClusteringKeyValue);
					String[] header1 = headerAndTuple[0];
					String[] tuple = headerAndTuple[1];

					for (int j = 1; j < curpage.length; j++) {
						// loop till i know which one is in the deletion list get the
						// index from it
						for (int m = 0; m < header1.length; m++) {
							if (htblColNameValue.containsKey(header1[m])) {
								String oldVal = tuple[m];
								// change its value in the tuple
								tuple[m] = htblColNameValue.get(header1[m]).toString();
								// check if it is a foreign key in another table
								updateForeign(header1[m], tuple[m], oldVal);
							}
						}
						// write the updated tuple back to the table
						writeUpdatedTuple(curTable, tuple);
					}
					// write the updated tuple back to the table
					writeUpdatedTuple(curTable, tuple);
				}
			} else {
				// if no index is created on the table
				// search linearly through the entire table to find the required tuple
				String[][] headerAndTuple = findTuple(curTable, strClusteringKeyValue);
				String[] header = headerAndTuple[0];
				String[] tuple = headerAndTuple[1];

				// validate the input

				// find changed values and change them in the tuple
				for (int i = 0; i < header.length; i++) {
					if (htblColNameValue.containsKey(header[i])) {
						String oldVal = tuple[i];
						// change its value in the tuple
						tuple[i] = htblColNameValue.get(header[i]).toString();
						// check if it is a foreign key in another table
						updateForeign(header[i], tuple[i], oldVal);
					}
				}
				// write the updated tuple back to the table
				writeUpdatedTuple(curTable, tuple);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String[][] findTupleUsingIndex(Table table, ArrayList<String> afterSplit,
			String strClusteringKeyValue) throws DBAppException {
		String[][] res;
		for (int i = 0; i < afterSplit.size(); i++) {
			String[][] pages = reader.readCSV(table.getPath() + afterSplit.get(i));
			res = new String[pages.length][2];
			int clusterColIndex = 0;
			for (int h = 0; h < pages[0].length; h++) {
				if (table.getClusterCol().equals(pages[0][h]))
					clusterColIndex = h;
			}
			for (int j = 1; j < pages.length; j++) {
				if (strClusteringKeyValue.equals(pages[j][clusterColIndex])) {
					res[0] = pages[0];
					res[1] = pages[j];
					return res;
				}
			}
		}
		throw new DBAppException("Tuple not found in Table");
	}

	public static void updateForeign(String colName, String newVal, String oldVal) {
		String[][] metaData = reader.readCSV("metadata.csv");
		for (int i = 1; i < metaData.length; i++) {
			if (metaData[i][1].equals(colName) && metaData[i][8].equals("True")) {
				String tableName = metaData[i][0];
				Table table = tables.get(tableName);
				for (int j = 0; j < table.getPages().size(); j++) {
					String[][] page = reader.readCSV(table.getPages().get(j).getPath());
					int col = 0;
					for (int k = 0; k < page.length; k++) {
						if (k == 0) {
							String[] header = page[0];
							for (col = 0; col < header.length; col++) {
								if (header[col].equals(colName))
									break;
							}
						} else if (page[k][col].equals(oldVal)) {
							page[k][col] = newVal;
							writeUpdatedTuple(table, page[k]);
						}
					}
				}
			}
		}
	}

	// searches for the location of the old tuple
	// and replaces it with the new one
	public static void writeUpdatedTuple(Table table, String[] tuple) {
		for (int i = 0; i < table.getPages().size(); i++) {
			String[][] page = reader.readCSV(table.getPages().get(i).getPath());
			int clusterColIndex = 0;
			for (int h = 0; h < page[0].length; h++) {
				if (table.getClusterCol().equals(page[0][h])) {
					clusterColIndex = h;
				}
			}
			for (int j = 1; j < page.length; j++) {
				if (page[j][clusterColIndex].equals(tuple[clusterColIndex])) {
					page[j] = tuple;
					writer.writePage(table.getPages().get(i).getPath(), page);
				}
			}
		}
	}

	// searches linearly through all pages of the table to find the requested tuple
	// returns header and tuple
	public static String[][] findTuple(Table table, String strClusteringKeyValue) throws DBAppException {
		String[][] res;
		for (int i = 0; i < table.getPages().size(); i++) {
			String[][] page = reader.readCSV(table.getPages().get(i).getPath());
			res = new String[page.length][2];
			int clusterColIndex = 0;
			for (int h = 0; h < page[0].length; h++) {
				if (table.getClusterCol().equals(page[0][h]))
					clusterColIndex = h;
			}
			for (int j = 1; j < page.length; j++) {
				if (strClusteringKeyValue.equals(page[j][clusterColIndex])) {
					res[0] = page[0];
					res[1] = page[j];
					return res;
				}
			}
		}
		throw new DBAppException("Tuple not found in Table");
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		try {
			Table curTable = tables.get(strTableName);
			Hashtable<String, String> foreigns = new Hashtable<>();
			String[][] metaData = reader.readCSV("metadata.csv");
			for (int i = 0; i < metaData.length; i++) {
				if (htblColNameValue.containsKey(metaData[i][1]) && metaData[i][8].equals("True")
						&& !curTable.getName().equals(metaData[i][0])) {
					foreigns.put(metaData[i][1], metaData[i][0]);
				}
			}
			boolean exist = false;
			int gridindex = 0;
			for (int i = 0; i < curTable.getIndecies().size(); i++) {
				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String key = entry.getKey();
					if (curTable.getIndecies().get(i).getName().contains(key)) {
						exist = true;
						gridindex = i;
					}
				}
			}
			if (exist == true) {
				GridIndex cur = curTable.getIndecies().get(gridindex);
				String[] colNames = cur.getName().split("_");
				int numCols = 0;
				boolean first = false;
				String val1 = "";
				String val2 = "";
				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String key = entry.getKey();
					if (key.equals(colNames[0])) {
						numCols++;
						first = true;
						val1 = entry.getValue().toString();
					} else if (key.equals(colNames[1])) {
						numCols++;
						val2 = entry.getValue().toString();
					}
				}
				String[] pages;
				ArrayList<String> afterSplit = new ArrayList<>();
				String page = "";
				if (numCols == 1) {
					// get using cluster method and splitting
					pages = cur.getByCluster(first ? val1 : val2, first);
					for (int i = 0; i < pages.length; i++) {
						if (pages[i] == null)
							continue;
						String[] temp = pages[i].split("-");
						for (String s : temp)
							afterSplit.add(s);
					}
				} else {
					// numCols = 2
					// get method using column names
					page = cur.get(val1, val2);
					String[] temp = page.split("-");
					for (String s : temp)
						afterSplit.add(s);
				}
				HashSet<String> uniqueSet = new HashSet<>(afterSplit);
				afterSplit = new ArrayList<>(uniqueSet);
				System.out.println(afterSplit);
				for (int i = 0; i < afterSplit.size(); i++) {
					String[][] curpage = reader.readCSV(curTable.getPath() + afterSplit.get(i));
					String[] header = curpage[0];
					int clusterColIndex = 0;
					for (int h = 0; h < header.length; h++) {
						if (curTable.getClusterCol().equals(header[h]))
							clusterColIndex = h;
					}
					for (int j = 1; j < curpage.length; j++) {
						// loop till i know which one is in the deletion list get the
						// index from it
						for (int k = 0; k < curpage[j].length; k++) {

							// need to check for all input columns before deletion

							if (htblColNameValue.containsKey(header[k])) {
								Class colClass = htblColNameValue.get(header[k]).getClass();
								Constructor constructor = colClass.getConstructor(String.class);
								Object val = constructor.newInstance(curpage[j][k]);
								if (htblColNameValue.get(header[k]).equals(val))
									shiftupTuples(curTable, i, j);
							}
						}
					}
				}
			} else {
				// if no index is created on the table
				// search linearly through the entire table to find the required tuple/tuples
				for (int i = 0; i < curTable.getPages().size(); i++) {
					String[][] page = reader.readCSV(curTable.getPages().get(i).getPath());
					String[] header = page[0];
					int clusterColIndex = 0;
					for (int h = 0; h < header.length; h++) {
						if (curTable.getClusterCol().equals(header[h]))
							clusterColIndex = h;
					}
					for (int j = 1; j < page.length; j++) {
						// loop till i know which one is in the deletion list get the
						// index from it
						for (int k = 0; k < page[j].length; k++) {

							// need to check for all input columns before deletion

							if (htblColNameValue.containsKey(header[k])) {
								Class colClass = htblColNameValue.get(header[k]).getClass();
								Constructor constructor = colClass.getConstructor(String.class);
								Object val = constructor.newInstance(page[j][k]);
								if (htblColNameValue.get(header[k]).equals(val)) {
									String[] tuple = shiftupTuples(curTable, i, j);
									deleteForeign(foreigns, header, tuple);
								}
							}
						}
					}
				}
				// }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void deleteForeign(Hashtable<String, String> foreigns, String[] header, String[] tuple) {
		for (int i = 0; i < header.length; i++) {
			if (foreigns.containsKey(header[i])) {
				Table cur = tables.get(foreigns.get(header[i]));
				for (int j = 0; j < cur.getPages().size(); j++) {
					String[][] page = reader.readCSV(cur.getPages().get(i).getPath());
					String[] pageHeader = page[0];
					int pageInd = 0;
					for (int k = 0; k < pageHeader.length; k++) {
						if (pageHeader[k].equals(header[i]))
							pageInd = k;
					}
					boolean found = false;
					for (int k = 1; k < page.length; k++) {
						if (page[k][pageInd].equals(tuple[i])) {
							page[k][pageInd] = null;
							writer.writePage(cur.getPages().get(i).getPath(), page);
							found = true;
							break;
						}
					}
					if (found)
						break;
				}
			}
		}
	}

	// shift tuples in the table for deletion method
	public static String[] shiftupTuples(Table table, int pageNum, int index) {// <------------------doesnt make the
																				// last attribute
		// empty
		int Req_row = index;
		String[][] page = reader.readCSV(table.getPages().get(pageNum).getPath());
		String[] tuple = page[index];
		if (page.length > 2) {
			// first reserved for the labels and the second for the first and only attribute
			// which will be deleted
			int j;
			for (j = Req_row; j < page.length - 1; j++) {// shift from the required row till the end
				page[j] = page[j + 1];
			}
			page[page.length - 1] = null;
			String filePath = table.getPages().get(pageNum).getPath();
			writer.writePage(filePath, page);
		} else {
			try {
				Path path = Paths.get(table.getPages().get(pageNum).getPath());
				Files.delete(path);
			} catch (IOException e) {
				System.out.println("An error occurred while deleting the file: " + e.getMessage());
			}
		}

		return tuple;
	}// <--------------------------------------------------------------------------------------------------------------------------------------------------------->

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("No parameters passed");
		if (arrSQLTerms.length != 1 && strarrOperators.length != arrSQLTerms.length - 1)
			throw new DBAppException("Insufficient parameters");
		if (!tables.containsKey(arrSQLTerms[0].get_strTableName()))
			throw new DBAppException(arrSQLTerms[0].get_strTableName() + " table does not exist");
		for (int i = 0; strarrOperators != null && i < strarrOperators.length; i++) {
			if (!strarrOperators[i].equals("AND") && !strarrOperators[i].equals("OR"))
				throw new DBAppException(
						strarrOperators[i] + " is not a supported operator (only AND and OR are supported)");
		}
		Table t = tables.get(arrSQLTerms[0].get_strTableName());
		if (t.getPages().size() == 0)
			return null;
		try {
			int[] gridindecies = new int[arrSQLTerms.length * 2];
			Arrays.fill(gridindecies, -1);
			for (int j = 0; j < arrSQLTerms.length; j++) {
				for (int k = 0; k < t.getIndecies().size(); k++) {
					String[] column = t.getIndecies().get(k).getName().split("_");
					if (column[0].equals(arrSQLTerms[j].get_strColumnName())) {
						gridindecies[j * 2] = k;
						gridindecies[j * 2 + 1] = 0;
					} else if (column[1].equals(arrSQLTerms[j].get_strColumnName())) {
						gridindecies[j * 2] = k;
						gridindecies[j * 2 + 1] = 1;
					} else
						gridindecies[j] = -1;
				}
			}
			LinkedList<LinkedList<String[]>> SQLres = new LinkedList<>();
			String[] supportedOperators = { ">", ">=", "<", "<=", "=" };

			// read the metadata associated with the table
			String tableName = t.getName();
			String[][] metadata = reader.readCSV("metadata.csv");
			String[][] tableMeta = reader.readTableMeta(metadata, tableName);
			String[] header = new String[tableMeta.length];
			for (int i = 0; i < tableMeta.length; i++) {
				header[i] = tableMeta[i][1];
			}
			for (int i = 0; i < arrSQLTerms.length; i++) {
				// read sql term and validate its object type store it in the arrayList
				SQLTerm cur = arrSQLTerms[i];
				String colName = cur.get_strColumnName();
				String operator = cur.get_strOperator();
				Object value = cur.get_objValue();
				// read metadata to find out the data type of the column
				// validate that the value is of that type
				// check if operator is one of the supported operators
				if (!listContains(operator, supportedOperators))
					throw new DBAppException(operator + " is not a supported operator");
				String type = "";
				Class colClass = null;
				for (int j = 0; j < tableMeta.length; j++) {
					if (tableMeta[j][1].equals(colName)) {

						// handle the date case

						type = tableMeta[j][2];
						colClass = Class.forName(type);
						if (!colClass.equals(value.getClass()))
							throw new DBAppException("Incompatible data types");
						break;
					}
				}
				if (gridindecies[i * 2] != -1) {
					GridIndex grid = t.getIndecies().get(gridindecies[i * 2]);
					int curColumn = gridindecies[i * 2 + 1];
					ArrayList<String> temp = grid.getByRange(arrSQLTerms[i].get_objValue().toString(), curColumn == 0,
							arrSQLTerms[i].get_strOperator());
					HashSet<String> hset = new HashSet<>();
					for (String s : temp) {
						if (s == null)
							continue;
						String[] t1 = s.split("-");
						for (String ss : t1)
							hset.add(ss);
					}
					ArrayList<String> pages = new ArrayList<>(hset);
					Collections.sort(pages);
					System.out.println("***");
					System.out.println(pages);
					System.out.println("***");
					LinkedList<String[]> res = new LinkedList<>();
					for (int j = 0; j < pages.size(); j++) {
						String[][] page = reader.readCSV(t.getPath() + pages.get(j));
						header = page[0];
						int index = 0;
						for (index = 0; index < header.length; index++) {
							if (header[index].equals(colName))
								break;
						}
						for (int row = 1; row < page.length; row++) {
							String[] tuple = page[row];
							if (checkCondition(tuple[index], operator, value, type))
								res.add(tuple);
						}
					}
					SQLres.add(res);
				} else {
					// if no index is created on the table
					LinkedList<String[]> res = new LinkedList<>();
					for (int j = 0; j < t.getPages().size(); j++) {
						String[][] page = reader.readCSV(t.getPages().get(j).getPath());
						header = page[0];
						int index = 0;
						for (index = 0; index < header.length; index++) {
							if (header[index].equals(colName))
								break;
						}
						for (int row = 1; row < page.length; row++) {
							String[] tuple = page[row];
							if (checkCondition(tuple[index], operator, value, type))
								res.add(tuple);
						}
					}
					SQLres.add(res);
				}
				int opIndex = 0;
				String clusterCol = t.getClusterCol();
				int clusterIndex = 0;
				for (int w = 0; w < header.length; w++) {
					if (header[w].equals(clusterCol))
						clusterIndex = w;
				}
				while (SQLres.size() > 1) {
					LinkedList<String[]> first = SQLres.removeFirst();
					LinkedList<String[]> second = SQLres.removeFirst();
					LinkedList<String[]> result = new LinkedList<>();
					String sqlOperator = strarrOperators[opIndex++];
					Constructor constructor = colClass.getConstructor(String.class);
					if (sqlOperator.equals("AND")) {
						int a = 0;
						int b = 0;
						while (a < first.size() && b < second.size()) {
							Object f = constructor.newInstance(first.get(a)[clusterIndex]);
							Object s = constructor.newInstance(second.get(b)[clusterIndex]);
							if (((Comparable) f).compareTo(s) < 0) {
								a++;
							} else if (((Comparable) f).compareTo(s) > 0) {
								b++;
							} else {
								result.add(first.get(a));
								a++;
								b++;
							}
						}
					} else {
						int a = 0;
						int b = 0;
						while (a < first.size() && b < second.size()) {
							Object f = constructor.newInstance(first.get(a)[clusterIndex]);
							Object s = constructor.newInstance(second.get(b)[clusterIndex]);
							if (((Comparable) f).compareTo(s) < 0) {
								result.add(first.get(a));
								a++;
							} else if (((Comparable) f).compareTo(s) > 0) {
								result.add(second.get(b));
								b++;
							} else {
								result.add(first.get(a));
								a++;
								b++;
							}
						}
					}
					SQLres.addFirst(result);
				}
			}
			SQLres.getFirst().addFirst(header);
			return SQLres.getFirst().iterator();

		} catch (ClassNotFoundException | SecurityException | NoSuchMethodException | InstantiationException
				| IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static boolean checkCondition(String tupleValue, String operator, Object value, String type)
			throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class colClass = Class.forName(type);
		Constructor constructor = colClass.getConstructor(String.class);
		Object tupleVal = constructor.newInstance(tupleValue);
		switch (operator) {
		case ">":
			if (((Comparable) tupleVal).compareTo(value) > 0)
				return true;
			break;
		case ">=":
			if (((Comparable) tupleVal).compareTo(value) >= 0)
				return true;
			break;
		case "<=":
			if (((Comparable) tupleVal).compareTo(value) <= 0)
				return true;
			break;
		case "<":
			if (((Comparable) tupleVal).compareTo(value) < 0)
				return true;
			break;
		case "=":
			if (((Comparable) tupleVal).compareTo(value) == 0)
				return true;
			break;
		}
		return false;
	}

	// prints a 2d array of strings
	public static void printGrid(String[][] grid) {
		for (String[] row : grid) {
			if (row[0] == null)
				break;
			for (String element : row) {
				System.out.print(element + " ");
			}
			System.out.println();
		}
	}

	// prints an array of strings
	public static void printArray(String[] arr) {
		for (String s : arr) {
			System.out.print(s + " ");
		}
		System.out.println();
	}

	// converts an array to a String in csv format
	public static String convertToString(String[] arr) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < arr.length; i++) {
			sb.append(arr[i]);
			if (i != arr.length - 1)
				sb.append(",");
		}
		return sb.toString();
	}

	public static void serialize(Table t) {
		try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(t.getName() + ".ser"));
			out.writeObject(t);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void serialize(GridIndex g) {
		try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(System.getProperty("user.dir")
					+ File.separator + "_Indecies" + File.separator + g.getName() + ".ser"));
			out.writeObject(g);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void deserializeAll() {
		File folder = new File(System.getProperty("user.dir"));
		File[] files = folder.listFiles();
		for (File file : files) {
			if (file.isFile() && file.getName().endsWith(".ser")) {
				try {
					ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
					Table obj = (Table) in.readObject();
					tables.put(obj.getName(), obj);
					String filePath = System.getProperty("user.dir") + File.separator + obj.getName();
					// load pages
					File tableFolder = new File(filePath);
					File[] tableFiles = tableFolder.listFiles();
					for (File f : tableFiles) {
						obj.getPages().add(f);
					}
					in.close();
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		folder = new File(System.getProperty("user.dir") + File.separator + "_Indecies");
		files = folder.listFiles();
		for (File file : files) {
			if (file.isFile() && file.getName().endsWith(".ser")) {
				try {
					ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
					GridIndex obj = (GridIndex) in.readObject();
					obj.getOnTable().getIndecies().add(obj);
				} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}
}