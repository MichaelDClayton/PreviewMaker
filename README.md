# PreviewMaker

Multithreaded app that extracts 90 seconds of footage from video.
-BlockingQueue
-CountDownLatch

Makes use of ffmpeg linux command line tool.

Originally, this project used Xuggler to generate 90 seconds previews, took too long.
About 1 week for 1500 files. Using ffmpeg with threads take little over 1 hour. Most files sizes are below 1GB...50MB, etc...

