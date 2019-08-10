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
		
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		// �����ڼ�Ⱥ������
		 configuration.set("fs.defaultFS", "hdfs://192.168.91.151:9000");
		 FileSystem fs = FileSystem.get(configuration);

	;
		
		// 2 ����Ŀ¼
		 fs.mkdirs(new Path("/1223"));

		
		// 3 �ر���Դ
		fs.close();
	}
	
	
	/*�ϴ��ļ�*/
	
	
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

		//1.��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. ����������
		FileInputStream fis = new FileInputStream("d:/hello.txt");
		
		//3. ���������
		FSDataOutputStream fos = fileSystem.create(new Path("/hello.txt"));
		
		
		//4. ���Ŀ���
		 IOUtils.copyBytes(fis, fos, configuration);
		//5. �ر���Դ
		 fileSystem.close();
		 
		 System.out.println("over");
		
	}
	
            /*	
             * �ļ�����
             * 
             */
	@Test
	public void testGet2() throws IOException, InterruptedException, URISyntaxException{
		//1. ��ȡhdfs�Ŀͻ���
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. ��ȡ������
		FSDataInputStream fis = fileSystem.open(new Path("/hello.txt"));
		
		//3. ��ȡ�����
		FileOutputStream fos = new FileOutputStream("d:/hello2222.text");
		
		//4. �ļ�����
		IOUtils.copyBytes(fis, fos, configuration);
		//5. �ر���Դ
		fileSystem.close();
		
		System.out.println("over2");
	}
	
/*	
 * 
 * ��ȡhdfs�ϵ��ļ��飨��һ�飩
 */
	@Test
	public void testSeek1() throws IOException, InterruptedException, URISyntaxException{
		//1. ��ȡhdfs�Ŀͻ���
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		
		//2. ��ȡ������
		FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.3.tar.gz"));
		
		//3. ��ȡ�����
		FileOutputStream fos = new FileOutputStream("d:/hadoop_partA");
		//4. ���Ŀ���
		byte[] buf = new byte[1024];
		for (int i = 0;i< 1024 * 128; i++){
			fis.read(buf);
			fos.write(buf);
		}
		
		//5. �ر���Դ
		fileSystem.close();
		System.out.println("��ȡ�ļ����һ���������");
	}
	
	/*	
	 * 
	 * ��ȡhdfs�ϵ��ļ��飨�ڶ��飩
	 */
@Test
	public void testSeek2() throws IOException, InterruptedException, URISyntaxException{
		//1. ��ȡhdfs�Ŀͻ���
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
		//2. ��ȡ������
		FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.3.tar.gz"));
		//3. ��ȡ�����
		FileOutputStream  fos = new FileOutputStream("/hadoop_partB");
		//4. ���Ŀ���
		fis.seek(1024*1024*128);      //ָ��ָ����128MB,���ӵڶ��鿪ʼ
		IOUtils.copyBytes(fis, fos, configuration);
	    //5. �ر���Դ
		fileSystem.close();
		
		System.out.println("��ȡ�ļ���ڶ����������");
	}


/*
�ݹ���ʾ�ļ�

*/
@Test
	
	public void listFile() throws IOException, InterruptedException, URISyntaxException{
	//1. ��ȡhdfs�ͻ���
	Configuration configuration = new Configuration();
	FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
	
/*	����·������ʾ��ʾĳ��·���µ��ļ����б�
	������·���������ļ���Ŀ¼��Ԫ���ݷŵ�һ��FileStatus��������
	FileStatus�����װ���ļ���Ŀ¼��Ԫ���ݣ������ļ����ȡ����С��Ȩ�޵���Ϣ*/
	
	FileStatus[] fileStatusArray = fileSystem.listStatus(new Path("/user"));
	for (int i = 0; i < fileStatusArray.length; i++){
		FileStatus fileStatus = fileStatusArray[i];
		
		//���ȼ�⵱ǰ�Ƿ����ļ��У��������ݹ�
		if(fileStatus.isDirectory()){
			System.out.println("��ǰ·���ǣ� " + fileStatus.getPath());
			listFile2(fileStatus.getPath());
		}else {
			System.out.println("��ǰ·����: " + fileStatus.getPath());
		}
	}
	System.out.println("·���ѻ�ȡ���");
}

public void listFile2(Path path) throws IOException, InterruptedException, URISyntaxException{
	//1. ��ȡhdfs�ͻ���
	Configuration configuration = new Configuration();
	FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.151:9000"), configuration, "ccgui");
	
/*	����·������ʾ��ʾĳ��·���µ��ļ����б�
	������·���������ļ���Ŀ¼��Ԫ���ݷŵ�һ��FileStatus��������
	FileStatus�����װ���ļ���Ŀ¼��Ԫ���ݣ������ļ����ȡ����С��Ȩ�޵���Ϣ*/
	
	FileStatus[] fileStatusArray = fileSystem.listStatus(path);
	for (int i = 0; i < fileStatusArray.length; i++){
		FileStatus fileStatus = fileStatusArray[i];
		
		//���ȼ�⵱ǰ�Ƿ����ļ��У��������ݹ�
		if(fileStatus.isDirectory()){
			System.out.println("��ǰ·���ǣ� " + fileStatus.getPath());
			listFile2(fileStatus.getPath());
		}else {
			System.out.println("��ǰ·����: " + fileStatus.getPath());
		}
	}

}




	
}