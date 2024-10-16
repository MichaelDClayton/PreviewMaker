package previews;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.PropertyConfigurator;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.S3Object;
import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.IMediaWriter;
import com.xuggle.mediatool.ToolFactory;

import previews.CutChecker;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class Worker implements Runnable {
	private Queue<String> queue;
	private CountDownLatch startSignal;
	private CountDownLatch doneSignal;

	static final Logger errorLogger = Logger.getLogger("errorLogger");
	static final Logger infoLogger = Logger.getLogger("infoLogger");
	
	
	public Worker(Queue<String> queue, String conferenceYear, CountDownLatch startSignal, CountDownLatch doneSignal) {
		this.queue = queue;
		this.startSignal = startSignal;
		this.doneSignal = doneSignal;
	}

	@Override
	public void run() {
		try {
			this.startSignal.await();
		} catch (InterruptedException e1) {
			//e1.printStackTrace();
			errorLogger.error("latch start signal error: ",e1);
		}

		// log4j configuration
		PropertyConfigurator.configure("log4j.properties");

		while (!queue.isEmpty()) {

			String fileName = queue.poll() + ".mp4";

			File f = new File("/file/path/preview_" + fileName);
			if (fileName != null && !f.exists()) {
				System.out.println("Processing File " + fileName + "....");

				/**
				 * preview making with java and xuggle
				 */
				/*
				 * IMediaReader reader = ToolFactory.makeReader(fileName);
				 * CutChecker cutChecker = new CutChecker();
				 * reader.addListener(cutChecker); IMediaWriter writer =
				 * ToolFactory.makeWriter("preview_" + fileName, reader);
				 * cutChecker.addListener(writer); Boolean updated = false; try{
				 * while (reader.readPacket() == null) { if
				 * ((cutChecker.timeInMilisec >= 90 * 1000000) && (!updated)) {
				 * cutChecker.removeListener(writer); writer.close(); writer =
				 * ToolFactory.makeWriter("_original" +fileName, reader);
				 * cutChecker.addListener(writer); updated = true; } }
				 * }catch(RuntimeException re){ logger.info(re); continue; }
				 */

				/**
				 * previewin making with command line ffmpeg.
				 */

				Process p = null;
				try {
					p = Runtime.getRuntime()
							.exec("ffmpeg -ss 0 -t 90 -i " + fileName + " -strict -2 " + "preview_" + fileName);

				} catch (IOException e) {
					e.printStackTrace();
					errorLogger.error("shell script error!",e);
				}

				try {
					p.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
					errorLogger.error("Latch Issue!",e);
				}
			
			
				fileName = null;
			}else{
				infoLogger.info("File Missing:"+fileName);
			}
		}

		this.doneSignal.countDown();

	}

}
