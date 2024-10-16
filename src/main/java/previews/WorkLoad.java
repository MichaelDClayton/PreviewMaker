package previews;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.log4j.Logger;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;

import DB;
import report.Status;

public class WorkLoad {

	private final static String sql = "query to get product ids";
	static final Logger errorLogger = Logger.getLogger("errorLogger");
	static final Logger infoLogger = Logger.getLogger("infoLogger");
	private static int NUMBER_OF_THREADS = 5;

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		BlockingQueue<String> filesToDownload = new LinkedBlockingDeque<String>(1024);
		BlockingQueue<String> filesToPreview = new LinkedBlockingDeque<String>(1024);
		BlockingQueue<String> filesToUpload = new LinkedBlockingDeque<String>(1024);
		
		String currentYear = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
		
		// DB connection.
		DB db = new DB();
		PreparedStatement st = null;
		Connection conn = null;
		conn = db.getConnection();

		// get ids from ats_store.products.
		try {
			
			st = conn.prepareStatement(sql);
			st.setString(1, currentYear);
			ResultSet rs = st.executeQuery();
			// add each id to IDS.
			while (rs.next()) {
				filesToDownload.add(rs.getString("product_id"));
				filesToPreview.add(rs.getString("product_id"));
				filesToUpload.add(rs.getString("product_id"));
			}
			conn.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		int recordSize = filesToDownload.size();
		
		
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		/**
		 * Here we can find out how many cores we have.
		 * Then make the number of threads NUMBER_OF_THREADS = the number of cores.
		 */
		NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors();
		System.out.println("Thread Count: "+NUMBER_OF_THREADS);
		
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		/**
		 * 
		 * CountDownLatch.we use this to block main until the working threads
		 * are done working.
		 */
		
		CountDownLatch startSignal = new CountDownLatch(1);
		CountDownLatch doneSignal = new CountDownLatch(NUMBER_OF_THREADS);

		// start up the Status Object class. For Reporting.
		Thread statusThread = new Thread(new Status(filesToDownload, currentYear, recordSize, "DOWNLOADING..."));
		statusThread.start();

		/**
		 * prep downloader threads
		 */
		Thread[] workers = new Thread[NUMBER_OF_THREADS];
		for (int x = 0; x < NUMBER_OF_THREADS; x++) {
			workers[x] = new Thread(new S3ObjectDownloader(filesToDownload, currentYear, startSignal, doneSignal));
			workers[x].start();
		}

		// start threads.
		startSignal.countDown();

		// block Main, wait for theads to complete.
		try {
			doneSignal.await();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		
		
		// re-initialize CountDownLatch
		startSignal = new CountDownLatch(1);
		doneSignal = new CountDownLatch(NUMBER_OF_THREADS);

		// re-initialize the status object.
		statusThread = new Thread(new Status(filesToPreview, currentYear, recordSize, "PREVIEWING..."));
		statusThread.start();

		/**
		 * prep preview making threads.
		 */
		for (int x = 0; x < NUMBER_OF_THREADS; x++) {
			workers[x] = new Thread(new Worker(filesToPreview, currentYear, startSignal, doneSignal));
			workers[x].start();
		}

		// start threads.
		startSignal.countDown();

		// block Main, unti threads are complete.
		try {
			doneSignal.await();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
		
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		// we need the TransferManager for the uploads.
		TransferManager txManager = new TransferManager(new ClasspathPropertiesFileCredentialsProvider());

		// re-initialize CountDownLatch
		startSignal = new CountDownLatch(1);
		doneSignal = new CountDownLatch(NUMBER_OF_THREADS);

		// re-initialize status object.
		statusThread = new Thread(new Status(filesToUpload, currentYear, recordSize, "UPLOADING..."));
		statusThread.start();

		// prep s3 uploader threads.
		for (int x = 0; x < NUMBER_OF_THREADS; x++) {
			workers[x] = new Thread(
					new S3ObjectUploader(filesToUpload, currentYear, txManager, startSignal, doneSignal));
			workers[x].start();
		}

		// start threads.
		startSignal.countDown();
		try {
			doneSignal.await();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// shutdown transfer manager
		txManager.shutdownNow();
		
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		/**
		 * Cleanup by deleting *.mp4 files.
		 */
		System.out.println("Cleaning up ...");
		Process p = null;
		try {
			p = Runtime.getRuntime()
					.exec(new String[]{"bash","-c","rm -f /home/ubuntu/*.mp4"});

		} catch (IOException e) {
			//e.printStackTrace();
			errorLogger.error("shell script error, problem deleting *.mp4!",e);
		}
		
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			//e.printStackTrace();
			errorLogger.error("shell script error, problem waiting for rm- f  /home/ubuntu/*.mp4!",e);
		}
		
		long endTime = System.currentTimeMillis();
		long totalTimeSeconds = (endTime - startTime)/1000;
		long totalTimeMinutes = (endTime - startTime)/(1000 * 60);
		long totalTimeHours = (endTime - startTime)/(1000 * 60 * 60);
		
		System.out.println("Total Time in seconds :"+totalTimeSeconds);
		System.out.println("Total Time in minutes :"+totalTimeMinutes);
		System.out.println("Total Time in hours :"+totalTimeHours);
	}

}
