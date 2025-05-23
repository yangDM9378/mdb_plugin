import java.io.File;
import java.sql.*;

public class MariadbProcessor {
    private static final String JDBC_URL = ConfigUtil.getMariaDbPath();
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    public static void saveDataToMariaDB(String tableName, ResultSet rs) {
        Connection conn = null;

        try {
//            System.out.println("🔌 MariaDB 연결 시도 중...");
            conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
//            System.out.println("✅ MariaDB 연결 성공!");
            Logger.log( "MariaDB connection successfully");
            // 테이블 없으면 생성
            if (!tableExists(conn, tableName)) {
                System.out.println("테이블 & 키 생성" + tableName);
                createTableFromMetadata(conn, tableName, rs);
            }
            insertAllData(conn, tableName, rs);
        } catch (Exception e) {
            System.err.println("❌ MariaDB 오류 발생 (table: " + tableName + ")");
            Logger.log( "MariaDB error (table: " + tableName + ")");
            e.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (SQLException e) {
                Logger.log( "MariaDB connection fail");
                System.err.println("⚠️ 연결 종료 실패");
            }
        }
    }

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }

    private static void createTableFromMetadata(Connection conn, String tableName, ResultSet rs) throws SQLException {
        ResultSetMetaData rsMeta = rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        StringBuilder createSQL = new StringBuilder("CREATE TABLE `" + tableName + "` (");

        for (int i = 1; i <= columnCount; i++) {
            String colName = rsMeta.getColumnName(i);
            int colType = rsMeta.getColumnType(i);
            String colTypeName = mapSqlTypeToMariaDB(colType);

            createSQL.append("`").append(colName).append("` ").append(colTypeName);
            if (i < columnCount) createSQL.append(", ");
        }
        createSQL.append(")");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL.toString());
            System.out.println("🆕 테이블 생성 완료: " + tableName);
        } catch (SQLException e) {
            System.out.println("X 테이블 생성 실패: " + tableName);
        }
    }
    private static String mapSqlTypeToMariaDB(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
                return "INT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.FLOAT:
                return "FLOAT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.VARCHAR:
                return "VARCHAR(255)";
            case Types.CHAR:
                return "CHAR(1)";
            case Types.DATE:
                return "DATE";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BOOLEAN:
                return "BOOLEAN";
            default:
                return "TEXT";
        }
    }

    private static void insertAllData(Connection conn, String tableName, ResultSet rs) throws SQLException {
        ResultSetMetaData rsMeta = rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();

        StringBuilder sql = new StringBuilder("INSERT INTO `" + tableName + "` (");
        for (int i = 1; i <= columnCount; i++) {
            sql.append("`").append(rsMeta.getColumnName(i)).append("`");
            if (i < columnCount) sql.append(", ");
        }
        sql.append(") VALUES (");
        for (int i = 1; i <= columnCount; i++) {
            sql.append("?");
            if (i < columnCount) sql.append(", ");
        }
        sql.append(")");

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    pstmt.setObject(i, rs.getObject(i));
                }
                pstmt.addBatch();
                rowCount++;

                if (rowCount % 1000 == 0) { // 배치 최적화
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch(); // 남은 배치 처리
            Logger.log( "데이터 삽입 완료: " + rowCount + " rows");
            System.out.println("> Insert Data Count: " + rowCount + " rows");
        } catch (SQLException e) {
            Logger.log( "데이터 삽입 실패 (table: " + tableName + ")");
            System.err.println("x Insert fail (table: " + tableName + ")");
            throw e;
        }
    }
}
