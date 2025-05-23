import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MdbProcessor {
    private static final File LAST_TIME_FILE = new File("./LOGFILE_DIR/last_processed_time.Log");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static void readMdbBetweenLastProcessedTime(String currentTimeStr) {

        try {
            String mdbPath = ConfigUtil.getMdbPath();
            System.out.println("MDB PATH: " + mdbPath);

            Date currentTime = sdf.parse(currentTimeStr);
            Date lastTime = loadLastProcessedTime(currentTime);

            // 1. mdb 연결
            String url = "jdbc:ucanaccess://" + mdbPath;
            Connection conn = DriverManager.getConnection(url);
            Logger.log(">> MDB connection successful");

            DatabaseMetaData meta = conn.getMetaData();

            ResultSet tables = meta.getTables(null, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                ResultSet columns = meta.getColumns(null, null, tableName, "MEAS_DATE");
                if (columns.next()) {
                    System.out.println("📄 조회할 테이블: " + tableName);
                    Logger.log(" Search Table: " + tableName);
                    String sql = String.format(
                            "SELECT * FROM [%s] WHERE MEAS_DATE > ? AND MEAS_DATE <= ?", tableName
                    );
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    stmt.setTimestamp(1, new Timestamp(lastTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(currentTime.getTime()));

                    ResultSet rs = stmt.executeQuery();
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    MariadbProcessor.saveDataToMariaDB(tableName, rs);

                    // 확인용
                    int columnCount = rsMeta.getColumnCount();
                    while (rs.next()) {
                        System.out.print("🧪 [" + tableName + "] ");
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsMeta.getColumnName(i);
                            Object value = rs.getObject(i);
                            System.out.print(colName + ": " + value + " ");
                        }
                        System.out.println();
                    }

                    rs.close();
                    stmt.close();
                } else {
                    Logger.log( tableName + "MEAS_DATE column not found");
                    System.out.println("⏩ " + tableName + " 테이블에는 MEAS_DATE 컬럼이 없어 스킵합니다.");
                }
                columns.close();
            }
            tables.close();
            conn.close();

            System.out.println("조회 범위: " + sdf.format(lastTime) + " ~ " + sdf.format(currentTime));
            Logger.log("search success range: " + sdf.format(lastTime) + " ~ " + sdf.format(currentTime));
            saveLastProcessedTime(currentTime);

        } catch (Exception e) {
            Logger.log("MDB connection fail");
            e.printStackTrace();
        }
    }

    // 기존에 저장 완료되었던 시간 check
    private static Date loadLastProcessedTime(Date currentTime) {
        try {
            if (!LAST_TIME_FILE.exists()) {
                long oneDayMillis = 24 * 60 * 60 * 1000L;
                Date yesterday = new Date(currentTime.getTime() - oneDayMillis);
                try (FileWriter fw = new FileWriter(LAST_TIME_FILE)) {
                    fw.write(sdf.format(currentTime));
                }
                return yesterday;
            }
            // 파일이 존재하면 내용 읽기
            try (BufferedReader br = new BufferedReader(new FileReader(LAST_TIME_FILE))) {
                String timeStr = br.readLine();
                return sdf.parse(timeStr);
            }
        } catch (Exception e) {
            System.err.println("시간 로드 실패");
            return new Date(0);
        }
    }
    // 작업 완료 후 시간 갱신
    private static void saveLastProcessedTime(Date currentTime) {
        try (FileWriter fw = new FileWriter(LAST_TIME_FILE, false)) {
            fw.write(sdf.format(currentTime));
        } catch (IOException e) {
            System.err.println("마지막 처리 시간 저장 실패");
            e.printStackTrace();
        }
    }
}