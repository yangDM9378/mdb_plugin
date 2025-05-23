import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private static final long MAX_LOG_SIZE = 5 * 1024 * 1024; // 5MB
    private static final File LOG_FILE = new File("./LOGFILE_DIR/mdb_plugin.Log");
    private static final int MAX_BACKUP_COUNT = 10; // 롤링 파일은 최대 10개 유지

    public static synchronized void log(String msg) {
        try {
            rotateLogIfNeeded();

            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            try (FileWriter fw = new FileWriter(LOG_FILE, true)) {
                fw.write("[" + time + "] " + msg + "\n");
            }
        } catch (IOException e) {
            System.err.println("로그 기록 실패");
        }
    }

    private static void rotateLogIfNeeded() {
        if (LOG_FILE.length() < MAX_LOG_SIZE) return;

        // 오래된 로그 삭제
        File oldest = new File("mdb_plugin_" + MAX_BACKUP_COUNT + ".log");
        if (oldest.exists()) oldest.delete();

        // 롤링 파일 뒤로 밀기
        for (int i = MAX_BACKUP_COUNT - 1; i >= 1; i--) {
            File src = new File("mdb_plugin_" + i + ".log");
            File dest = new File("mdb_plugin_" + (i + 1) + ".log");
            if (src.exists()) src.renameTo(dest);
        }

        // 현재 로그를 mdb_plugin_1.log로 이동
        File firstBackup = new File("mdb_plugin_1.log");
        LOG_FILE.renameTo(firstBackup);

        // 새 로그 파일 생성
        try {
            LOG_FILE.createNewFile();
        } catch (IOException e) {
            System.err.println("새 로그 파일 생성 실패");
        }
    }
}