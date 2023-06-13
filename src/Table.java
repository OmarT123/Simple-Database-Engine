import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.text.AttributeSet;
import javax.swing.text.html.HTML.Tag;
import javax.swing.text.html.HTMLDocument.Iterator;

public class Table implements Serializable {
	private String name;
	private ArrayList<File> pages;
	private String path;
	private boolean hasIndex;
	private String clusterCol;
	private ArrayList<GridIndex> indecies;

	public Table(String name, String clusterCol) {
		this.name = name;
		this.clusterCol = clusterCol;
		this.pages = new ArrayList<>();
		this.indecies = new ArrayList<>();
		String folderName = name;
		this.path = System.getProperty("user.dir");
		File folder = new File(this.path + File.separator + folderName);
		if (!folder.exists()) {
			boolean created = folder.mkdirs();
			if (created) {
				//System.out.println("Folder created successfully.");
			} else {
				//System.out.println("Failed to create the folder.");
			}
		} else {
			//System.out.println("Folder already exists.");
		}
		this.path += "\\" + name + "\\";
		this.hasIndex = false;
	}

	
	public ArrayList<GridIndex> getIndecies() {
		return indecies;
	}

	public void setIndecies(ArrayList<GridIndex> indecies) {
		this.indecies = indecies;
	}

	public String getClusterCol() {
		return clusterCol;
	}

	public void setClusterCol(String clusterCol) {
		this.clusterCol = clusterCol;
	}

	public String getPath() {
		return path;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ArrayList<File> getPages() {
		return pages;
	}

	public void setPages(ArrayList<File> pages) {
		this.pages = pages;
	}

	public boolean hasIndex() {
		return hasIndex;
	}

	public void setHasIndex(boolean hasIndex) {
		this.hasIndex = hasIndex;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
