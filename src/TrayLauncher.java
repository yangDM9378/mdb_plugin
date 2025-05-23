import java.awt.*;
import java.io.File;

public class TrayLauncher {
    private static Thread watcherThread;
    private static LogWatcher watcher;
    private static TrayIcon trayIcon;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.log("MDB Plugin exit");
        }));

        if (!SystemTray.isSupported()) {
            System.err.println("System tray not supported!");
            return;
        }

        SystemTray tray = SystemTray.getSystemTray();
        Image image = Toolkit.getDefaultToolkit().getImage("tray_icon.png");

        PopupMenu popup = new PopupMenu();
        MenuItem logItem = new MenuItem("log");
        MenuItem exitItem = new MenuItem("exit");

        Logger.log(">> MDB Plugin start");

        watcher = new LogWatcher();
        watcherThread = new Thread(watcher);
        watcherThread.start();

        logItem.addActionListener(e -> {
            try {
                String logDirPath = "./LOGFILE_DIR";
                File logDir = new File(logDirPath).getAbsoluteFile();
                if (logDir.exists()) {
                    Desktop.getDesktop().open(logDir);
                } else {
                    Logger.log("not log Dir: " + logDir.getPath());
                }
            } catch (Exception ex) {
                Logger.log("Log Dir open fail");
                ex.printStackTrace();
            }
        });

        // 종료 버튼
        exitItem.addActionListener(e -> {
            if (watcher != null) {
                watcher.stop();
                watcherThread.interrupt();
            }
            tray.remove(trayIcon);
            Logger.log("MDB Plugin exit");
            System.exit(0);
        });

        // 트레이 메뉴 구성
        popup.add(logItem);
        popup.addSeparator();
        popup.add(exitItem);

        trayIcon = new TrayIcon(image, "mdb_plugin", popup);
        trayIcon.setImageAutoSize(true);

        try {
            tray.add(trayIcon);
        } catch (AWTException e) {
            System.err.println("TrayIcon 추가 실패");
        }
    }
}
