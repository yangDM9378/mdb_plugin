import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class LogWatcher implements Runnable {
    private volatile boolean running = true;
    private static final File LOG_FILE = new File(ConfigUtil.getLogFilePath());
    private static final File OFFSET_FILE = new File("./LOGFILE_DIR/position.Log");

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        Logger.log(">> LogWatcher start");

        try {
            long lastPosition = 0;
            if (OFFSET_FILE.exists()) {
                lastPosition = Long.parseLong(new BufferedReader(new FileReader(OFFSET_FILE)).readLine());
            } else {
                lastPosition = LOG_FILE.length();
                try (FileWriter writer = new FileWriter(OFFSET_FILE, false)) {
                    writer.write(Long.toString(lastPosition));
                } catch (IOException e) {
                    System.err.println("초기 position.Log 저장 실패");
                    e.printStackTrace();
                }
            }

            Path watchDir = LOG_FILE.toPath().getParent();
            WatchService watchService = FileSystems.getDefault().newWatchService();
            watchDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            while (running && !Thread.currentThread().isInterrupted()) {
                WatchKey key = watchService.take();

                List<WatchEvent<?>> events = key.pollEvents();
                for (WatchEvent<?> event : events) {
                    Path changed = (Path) event.context();
                    if (changed.endsWith(LOG_FILE.getName())) {
                        try (RandomAccessFile raf = new RandomAccessFile(LOG_FILE, "r")) {
                            if (LOG_FILE.length() < lastPosition) {
                                lastPosition = 0;
                            }
                            raf.seek(lastPosition);

                            String line;
                            while ((line = raf.readLine()) != null) {
                                String decodedLine = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                                if (decodedLine.contains("material processing state change:processed previous:inprocess material id:")) {
                                    System.out.println("→ " + decodedLine);
                                    String[] parts = decodedLine.split(" ");
                                    if (parts.length >= 2) {
                                        String currentTimeStr = parts[0] + " " + parts[1];
                                        try {
                                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                                            Date parsedTime = sdf.parse(currentTimeStr);
                                            MdbProcessor.readMdbBetweenLastProcessedTime(currentTimeStr);
                                        } catch (Exception e) {
                                            System.err.println("❌ 시간 파싱 실패: " + decodedLine);
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }

                            lastPosition = raf.getFilePointer();
                        }

                        try (FileWriter writer = new FileWriter(OFFSET_FILE, false)) {
                            writer.write(Long.toString(lastPosition));
                        }
                    }
                }

                if (!key.reset()) {
                    System.out.println("디렉토리 감시가 중단되었습니다.");
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Logger.log("LogWatcher exit");
    }
}