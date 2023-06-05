import java.io.File;
import java.util.ArrayList;

import javax.swing.text.AttributeSet;
import javax.swing.text.html.HTML.Tag;
import javax.swing.text.html.HTMLDocument.Iterator;

public class Table extends Iterator {
	private String name;
	private ArrayList<File> pages;
	private String path;
	private boolean hasIndex;
	
	public Table(String name) {
		this.name = name;
		this.pages = new ArrayList<>();
		String folderName = name;
		this.path = System.getProperty("user.dir");
		File folder = new File(this.path + File.separator + folderName);
		if (!folder.exists()) {
			boolean created = folder.mkdirs();
			if (created) {
				System.out.println("Folder created successfully.");
			} else {
				System.out.println("Failed to create the folder.");
			}
		} else {
			System.out.println("Folder already exists.");
		}
		this.path += "\\" + name + "\\";
		this.hasIndex = false;
 	}
	
	public String getPath()
	{	
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

	public AttributeSet getAttributes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getStartOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getEndOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void next() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Tag getTag() {
		// TODO Auto-generated method stub
		return null;
	}

}
