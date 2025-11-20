package benchmark.synchronize.components;

import java.util.concurrent.atomic.AtomicLong;
import java.util.Properties;

import benchmark.oltp.entity.statement.*;

import java.sql.*;

public class TPRecorder {
    private volatile boolean isFinished;
    private AtomicLong newOrder;
    private AtomicLong payment;
    private Connection dbConn;
    private StmtVodkaTime stmtVodkaTime;
    private PreparedStatement stmtUpdate;
    private Thread thread;

    public TPRecorder(int dbType, String database, Properties dbProps) {
        this.isFinished = false;
        try {
            dbConn = DriverManager.getConnection(database, dbProps);
            this.stmtVodkaTime = new StmtVodkaTime(dbConn, dbType);

            this.newOrder = new AtomicLong(0);
            this.payment = new AtomicLong(0);

            stmtUpdate = stmtVodkaTime.getStmtUpdateVodkaTime();

            thread = new Thread(this::updateVodkaTime);
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
            cleanup();
        }
    }

    private void updateVodkaTime() {
        while (!isFinished) {
            try {
                long deltaNewOrder = newOrder.getAndSet(0); 
                long deltaPayment = payment.getAndSet(0); 

                stmtUpdate.setLong(1, deltaNewOrder);
                stmtUpdate.setLong(2, deltaPayment);

                Thread.sleep(1000);
                stmtUpdate.executeUpdate();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break; 
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void addNeworder(int num) {
        newOrder.addAndGet(num);
    }

    public void addPayment(int num) {
        payment.addAndGet(num);
    }

    public void close() throws Throwable {
        isFinished = true;
        thread.interrupt();
        thread.join();
        cleanup();
    }

    private void cleanup() {
        try {
            if (stmtUpdate != null) {
                stmtUpdate.close();
            }
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}