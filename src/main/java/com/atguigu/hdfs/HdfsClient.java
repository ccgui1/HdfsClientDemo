package com.atguigu.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.httpclient.Cookie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.User;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsClient{
	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException{
		
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行
		 configuration.set("fs.defaultFS", "hdfs://192.168.91.151:9000");
		 FileSystem fs = FileSystem.get(configuration);

	;
		
		// 2 创建目录
		 fs.mkdirs(new Path("/1223"));

		
		// 3 关闭资源
		fs.close();
	}
	
	
	/*上传文件*/
	
	
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

		//1.获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. 创建输入流
		FileInputStream fis = new FileInputStream("d:/hello.txt");
		
		//3. 创建输出流
		FSDataOutputStream fos = fileSystem.create(new Path("/hello.txt"));
		
		
		//4. 流的拷贝
		 IOUtils.copyBytes(fis, fos, configuration);
		//5. 关闭资源
		 fileSystem.close();
		 
		 System.out.println("over");
		
	}
	
            /*	
             * 文件下载
             * 
             */
	@Test
	public void testGet2() throws IOException, InterruptedException, URISyntaxException{
		//1. 获取hdfs的客户端
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. 获取输入流
		FSDataInputStream fis = fileSystem.open(new Path("/hello.txt"));
		
		//3. 获取输出流
		FileOutputStream fos = new FileOutputStream("d:/hello2222.text");
		
		//4. 文件拷贝
		IOUtils.copyBytes(fis, fos, configuration);
		//5. 关闭资源
		fileSystem.close();
		
		System.out.println("over2");
	}
	
/*	
 * 
 * 获取hdfs上的文件块（第一块）
 */
	@Test
	public void testSeek1() throws IOException, InterruptedException, URISyntaxException{
		//1. 获取hdfs的客户端
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		
		//2. 获取输入流
		FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.3.tar.gz"));
		
		//3. 获取输出流
		FileOutputStream fos = new FileOutputStream("d:/hadoop_partA");
		//4. 流的拷贝
		byte[] buf = new byte[1024];
		for (int i = 0;i< 1024 * 128; i++){
			fis.read(buf);
			fos.write(buf);
		}
		
		//5. 关闭资源
		fileSystem.close();
		System.out.println("获取文件块第一部分已完成");
	}
	
	/*	
	 * 
	 * 获取hdfs上的文件块（第二块）
	 */
@Test
	public void testSeek2() throws IOException, InterruptedException, URISyntaxException{
		//1. 获取hdfs的客户端
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. 获取输入流
		FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.3.tar.gz"));
		//3. 获取输出流
		FileOutputStream  fos = new FileOutputStream("/hadoop_partB");
		//4. 流的拷贝
		fis.seek(1024*1024*128);      //指针指到了128MB,即从第二块开始
		IOUtils.copyBytes(fis, fos, configuration);
	    //5. 关闭资源
		fileSystem.close();
		
		System.out.println("获取文件块第二部分已完成");
	}


/*
递归显示文件

*/
@Test
	
	public void listFile() throws IOException, InterruptedException, URISyntaxException{
	//1. 获取hdfs客户端
	Configuration configuration = new Configuration();
	FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
	
/*	传入路径，表示显示某个路径下的文件夹列表
	将给定路径下所有文件和目录的元数据放到一个FileStatus的数组中
	FileStatus对象封装了文件和目录的元数据，包括文件长度、块大小、权限等信息*/
	
	FileStatus[] fileStatusArray = fileSystem.listStatus(new Path("/user"));
	for (int i = 0; i < fileStatusArray.length; i++){
		FileStatus fileStatus = fileStatusArray[i];
		
		//首先检测当前是否是文件夹，如果是则递归
		if(fileStatus.isDirectory()){
			System.out.println("当前路径是： " + fileStatus.getPath());
			listFile2(fileStatus.getPath());
		}else {
			System.out.println("当前路径是: " + fileStatus.getPath());
		}
	}
	System.out.println("路径已获取完毕");
}

public void listFile2(Path path) throws IOException, InterruptedException, URISyntaxException{
	//1. 获取hdfs客户端
	Configuration configuration = new Configuration();
	FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
	
/*	传入路径，表示显示某个路径下的文件夹列表
	将给定路径下所有文件和目录的元数据放到一个FileStatus的数组中
	FileStatus对象封装了文件和目录的元数据，包括文件长度、块大小、权限等信息*/
	
	FileStatus[] fileStatusArray = fileSystem.listStatus(path);
	for (int i = 0; i < fileStatusArray.length; i++){
		FileStatus fileStatus = fileStatusArray[i];
		
		//首先检测当前是否是文件夹，如果是则递归
		if(fileStatus.isDirectory()){
			System.out.println("当前路径是： " + fileStatus.getPath());
			listFile2(fileStatus.getPath());
		}else {
			System.out.println("当前路径是: " + fileStatus.getPath());
		}
	}

}




	
}