import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {
    private static final String CONFIG_FILE = "./mdb_plugin.config";
    private static final Properties props = new Properties();

    static {
        try (FileInputStream fis = new FileInputStream(CONFIG_FILE)) {
            props.load(fis);
        } catch (IOException e) {
            System.err.println("⚠ 설정 파일 읽기 실패: " + CONFIG_FILE);
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    public static String getMdbPath() {
        return getProperty("mdb_path");
    }

    public static String getLogFilePath() {
        return getProperty("log_file_path");
    }

    public static String getMariaDbPath() {
        return "jdbc:mariadb://" + getProperty("maria_db_path");
    }

    public static String getCheckLogValue() { return getProperty("check_log_value");}
}