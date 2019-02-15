import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.management.RuntimeErrorException;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Program {
	
	public static void main(String[] args) {
		
		if(args.length != 2){
			System.out.println("Please enter 2 arguments localPath, hdfsPath");
		}
		//TODO, Check if args are paths
		
		System.out.println(args[0]);
		String localPath = checkFilePath(args[0]);
		
		System.out.println(args[1]);
		String hdfsPath = checkFilePath(args[1]);
		
		String hdfsuri = "hdfs://quickstart.cloudera:8020/";
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsuri);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		
		try {
			FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
			syncFiles(fs, localPath, hdfsPath);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static String checkFilePath(String path){
		
		if(!path.endsWith("/")){
			path = path + "/";
		}
				
		return path;
	}
	
	public static void syncFiles(FileSystem fs, String localPath, String hdfsPath){
		
		
		File[] localFileList = getLocalFileNames(localPath);
		FileStatus[] hdfsFileList = getHDFSFileNames(fs, hdfsPath);
		
		//LOCAL
		List<String> localFileNames = new ArrayList<String>();
		System.out.println("Files in local Folder:");
		for (File file : localFileList){
			System.out.println(file);
			localFileNames.add(file.getName());
		}
		
		//HDFS
		List<String> hdfsFileNames = new ArrayList<String>();
		System.out.println("\nFiles in hdfs Folder");
		for (FileStatus file : hdfsFileList){
			System.out.println(file.getPath());
			hdfsFileNames.add(file.getPath().getName());
		}
		
		
		
		
		List<String> toBeDeleted = checkToBeDeleted(localFileNames, hdfsFileNames);
		deleteFromHDFS(fs, hdfsPath, toBeDeleted);
		
		List<String> toBeCopied = checkToBeCopied(localFileNames, hdfsFileNames);
		copyToHDFS(fs, hdfsPath, localPath, toBeCopied);
		
	}
	
	public static void copyToHDFS(FileSystem fs, String hdfsPath, String localPath, List<String> toBeCopied){
		
		for (String file : toBeCopied){
			try {
				fs.copyFromLocalFile(new Path(localPath + file), new Path(hdfsPath + file));
				System.out.println("\nCopied: " + localPath + file + " to: " + hdfsPath + file);

			} catch (IllegalArgumentException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static List<String> checkToBeCopied(List<String> localFileNames, List<String> hdfsFileNames){
		
		List<String> toBeCopied = new ArrayList<String>();		

		for (String localFile : localFileNames){
			boolean isPresent = false;
			for (String hdfsFile : hdfsFileNames){
				
				if(localFile.equals(hdfsFile)){
					isPresent = true;
				}
			}
			if(!isPresent){
				toBeCopied.add(localFile);
			}
		}
		
		return toBeCopied;
	}
	
	public static void deleteFromHDFS(FileSystem fs, String hdfsPath, List<String> toBeDeleted){
		
		for (String file : toBeDeleted){
			try {
				fs.delete(new Path(hdfsPath + file));
				System.out.println("\nDeleted: " + hdfsPath + file);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static List<String> checkToBeDeleted(List<String> localFileNames, List<String> hdfsFileNames){
				
		List<String> toBeDeleted = new ArrayList<String>();		

		for (String localFile : hdfsFileNames){
			boolean isPresent = false;
			for (String hdfsFile : localFileNames ){

				if (localFile.equals(hdfsFile)){
					isPresent = true;
				}
			}
			if(!isPresent){
				toBeDeleted.add(localFile);
			}
		}
		
		return toBeDeleted;
	}
	
	
	
	public static File[] getLocalFileNames(String localPath){
		
		File folder = new File(localPath);
		return folder.listFiles();
		
	}
	
	public static FileStatus[] getHDFSFileNames(FileSystem fs, String hdfsPath){
		
		try {
			return fs.listStatus(new Path(hdfsPath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

}