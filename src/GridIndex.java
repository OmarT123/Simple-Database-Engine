import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class GridIndex {
	private String name;
	private Table onTable;
	private String col1, col2;
	private String min1, max1, min2, max2;
	private boolean parsable = false;;
	private String[][] grid;

	public GridIndex(String name, Table table, String colType1, String col1, String min1, String max1, String colType2, String col2, String min2,
			String max2) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException {
		// verify table name
		// verify col names
		this.name = name;
		this.onTable = table;
		this.col1 = col1;
		this.col2 = col2;
		
		int N = 10; // store grid size in config file
		
		Class colClass1 = Class.forName(colType1);
		Class colClass2 = Class.forName(colType2);
		Constructor const1 = colClass1.getConstructor(String.class);
		Constructor const2 = colClass2.getConstructor(String.class);
		
		Object min1Obj = const1.newInstance(min1);
		Object max1Obj = const1.newInstance(max1);
		Object min2Obj = const1.newInstance(min2);
		Object max2Obj = const1.newInstance(max2);
		
		this.grid = new String[N + 1][N + 1];
		this.grid[0][0] = "     ";
		// fill headers
		int groupSize1 = (((Comparable) max1Obj).compareTo(min1Obj) + 1) / (N);
		int groupSize2 = (((Comparable) max2Obj).compareTo(min2Obj) + 1) / (N);
		Object j = min1Obj;
//		for (int i = 1; i < N; i++) {
//			String group = j.toString();
//			j += groupSize1;
//			j = Math.min(j, max1Int);
//			group += "-" + j;
//			this.grid[0][i] = group;
//			j++;
//		}
//		j = 0;
//		for (int i = 1; i < this.grid.length; i++) {
//			String group = "" + j;
//			j += groupSize2;
//			j = Math.min(j, max2Int);
//			group += "-" + j;
//			this.grid[i][0] = group;
//			j++;
//		}

		// fill data if available

		saveIndex();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Table getOnTable() {
		return onTable;
	}

	public String getCol1() {
		return col1;
	}

	public String getCol2() {
		return col2;
	}

	public String getMin1() {
		return min1;
	}

	public String getMax1() {
		return max1;
	}

	public String getMin2() {
		return min2;
	}

	public String getMax2() {
		return max2;
	}

	public boolean isParsable() {
		return parsable;
	}

	public void setParsable(boolean parsable) {
		this.parsable = parsable;
	}

	public String[][] getGrid() {
		return grid;
	}

	public void setGrid(String[][] grid) {
		this.grid = grid;
	}

	public static Object incrementObject(Object obj, Object amount) {
		Class curClass = obj.getClass();
		if (obj instanceof Integer) {
			Integer cur = ((Integer) obj);
			Integer am = ((Integer) amount);
			return cur + am;
		} else if (obj instanceof Double) {
			Double cur = ((Double) obj);
			Double am = ((Double) amount);
			return cur + am;
		} else if (obj instanceof String) {
			String cur = (String) obj;
			Integer am = (Integer)amount;
			for (int i = 0; i < am; i++) {
				cur = incrementLexicographically(cur);
			}
			return cur;
		} else {
		}
		return null;
 	}
	
	public static String incrementLexicographically(String input) {
	    char[] chars = input.toCharArray();
	    int length = chars.length;

	    for (int i = length - 1; i >= 0; i--) {
	        if (chars[i] != 'z') {
	            chars[i]++;
	            for (int j = i + 1; j < length; j++) {
	                chars[j] = 'a';
	            }
	            return new String(chars);
	        }
	    }
	    return input;
	}
	
	public void saveIndex() {
		String filePath = getOnTable().getPath() + getName();
		Writer.writePage(filePath, getGrid());
	}

	// methods: inse`rt, get, remove, contains

}
