package benchmark.synchronize.components;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import benchmark.synchronize.tasks.*;

import java.sql.*;

class HandleThread extends Thread {
    private ThreadPool threadPool;
    private ConnPool ApConnPool;
    private ConnPool TpConnPool;
    private HTAPCheckMetrics metrics;

    // statistics
    public Long finishedTaskNum;

    private int threadId;

    public HandleThread(ThreadPool threadPool, ConnPool ApConnPool, ConnPool TpConnPool, int threadId,
            HTAPCheckMetrics metrics) {
        this.threadPool = threadPool;
        this.ApConnPool = ApConnPool;
        this.TpConnPool = TpConnPool;
        this.threadId = threadId;
        this.finishedTaskNum = 0L;
        this.metrics = metrics;
    }

    public void run() {
        while (threadPool.isRunning()) {
            Task task = threadPool.getTask();
            if (!threadPool.isRunning()) {
                break;
            }
            ArrayList<Connection> conns = getConns(task.taskType);
            TaskResult taskResult = task.runTask(conns, threadId);
            metrics.add(taskResult);
            threadPool.closeTask(task);
            releaseConns(task.taskType, taskResult.isApConnErr, conns);
            finishedTaskNum += 1;
        }
        System.out.printf("[%d] end\n", threadId);
    }

    /* HandleThread.getConns() */
    private ArrayList<Connection> getConns(int taskType) {
        ArrayList<Connection> conns = new ArrayList<>();
        Connection apConn = ApConnPool.getConn();
        Connection tpConn = TpConnPool.getConn();

        // 对两条连接都设置并行度为 1
        try (Statement st1 = apConn.createStatement();
                Statement st2 = tpConn.createStatement()) {
            st1.execute("SET max_parallel_workers_per_gather = 256");
            // st2.execute("SET max_parallel_workers_per_gather = 256");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        conns.add(apConn);
        conns.add(tpConn);
        return conns;
    }

    private void releaseConns(int taskType, boolean isApConnErr, ArrayList<Connection> conns) {
        if (isApConnErr) {
            ApConnPool.destroyConn(conns.get(0));
        } else {
            ApConnPool.releaseConn(conns.get(0));
        }
    }
}

public class ThreadPool {
    // database connection:
    private ConnPool ApConnPool;
    private ConnPool TpConnPool;

    // task queue
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private Queue<Task> taskQueue;
    private int curTaskNum;
    private int curRunningTaskNum;

    // handle thread
    private int threadNum;
    private HandleThread handleThread[];

    // signal
    private boolean isRunning;

    // batchQueryPreparing is used for prevent multi thread execute batch query task
    // on the same (wid, did).
    private HashMap<Integer, HashMap<Integer, Boolean>> batchQueryPreparing = new HashMap<>();

    public ThreadPool(HTAPCheckInfo htapCheckInfo, Properties dbProps, HTAPCheckMetrics metrics) {
        this.isRunning = true;
        this.threadNum = htapCheckInfo.htapCheckApNum;
        this.curTaskNum = 0;
        this.curRunningTaskNum = 0;
        this.handleThread = new HandleThread[threadNum];
        this.taskQueue = new LinkedList<>();
        this.ApConnPool = new ConnPool(htapCheckInfo.htapCheckApNum + htapCheckInfo.htapCheckQueryNumber,
                htapCheckInfo.htapCheckApConn, dbProps);
        this.TpConnPool = new ConnPool(htapCheckInfo.htapCheckApNum + htapCheckInfo.htapCheckQueryNumber,
                htapCheckInfo.htapCheckTpConn, dbProps);
        for (int i = 0; i < threadNum; i++) {
            handleThread[i] = new HandleThread(this, this.ApConnPool, this.TpConnPool, i, metrics);
            handleThread[i].start();
        }
        for (int i = 1; i <= htapCheckInfo.warehouseNum; i++) {
            HashMap<Integer, Boolean> mp = new HashMap<>();
            for (int j = 1; j <= 10; ++j) {
                mp.put(j, false);
            }
            batchQueryPreparing.put(i, mp);
        }
    }

    public void trySpawn(Task task) {
        lock.lock();
        try {
            // 只有当没有任务正在执行时才添加新任务
            if (isRunning && curRunningTaskNum == 0) {
                taskQueue.add(task);
                curTaskNum++;
                condition.signal(); // 通知一个等待线程（不需要signalAll）
            }
        } finally {
            lock.unlock();
        }
    }

    public Task getTask() {
        lock.lock();
        try {
            while (taskQueue.isEmpty()) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // 重新设置中断状态
                    return null;
                }
                if (!isRunning) {
                    return null;
                }
            }
            curRunningTaskNum++; // 开始执行任务，计数器增加
            return taskQueue.remove();
        } finally {
            lock.unlock();
        }
    }

    public void closeTask(Task task) {
        lock.lock();
        try {
            curRunningTaskNum--; // 任务完成，计数器减少
            curTaskNum--;
            condition.signal(); // 通知等待中的线程任务已完成
        } finally {
            lock.unlock(); // 释放锁
        }
        try {
            // Thread.sleep(0);
            // Thread.sleep(1000);
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 如果线程被中断，重新设置中断状态
        }
    }

    // public void trySpawn(Task task) {
    // lock.lock();
    // boolean canSpawn = isRunning && curTaskNum < threadNum;
    // if (!canSpawn) {
    // lock.unlock();
    // return;
    // }

    // try {
    // taskQueue.add(task);
    // curTaskNum++;
    // condition.signalAll();
    // } finally {
    // lock.unlock();
    // }
    // }

    // public Task getTask() {
    // lock.lock();
    // try {
    // while (taskQueue.isEmpty()) {
    // try {
    // condition.await();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // if (!isRunning) {
    // return null;
    // }
    // }
    // return taskQueue.remove();
    // } finally {
    // lock.unlock();
    // }
    // }

    public void startTask() {
        lock.lock();
        curRunningTaskNum++;
        lock.unlock();
    }

    // public void closeTask(Task task) {
    // lock.lock();
    // curRunningTaskNum--;
    // curTaskNum--;
    // lock.unlock();
    // }

    public boolean isRunning() {
        lock.lock();
        boolean ret = isRunning;
        lock.unlock();
        return ret;
    }

    public boolean needSpawn() {
        lock.lock();
        boolean ret = isRunning && curTaskNum < threadNum;
        lock.unlock();
        return ret;
    }

    public void close() {
        lock.lock();
        try {
            isRunning = false;
            condition.signalAll(); // 唤醒所有等待的线程
        } finally {
            lock.unlock();
        }

        for (HandleThread thread : handleThread) {
            try {
                thread.join(); // 等待所有处理线程完成
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 重新设置中断状态
            }
        }

        ApConnPool.close();
        TpConnPool.close();
        System.out.println("ThreadPool closed and all connections are released.");
    }

    // public void close() throws Throwable {
    // lock.lock();
    // // prevent new task.
    // isRunning = false;
    // lock.unlock();
    // System.out.println("all task are finished");

    // // there is no tasks in TaskQueue, signal all threads and let them done.
    // for (int i = 0; i < threadNum; i++) {
    // lock.lock();
    // condition.signalAll();
    // lock.unlock();
    // }
    // for (int i = 0; i < threadNum; i++) {
    // long finishedTaskNum = handleThread[i].finishedTaskNum;
    // handleThread[i].join();
    // System.out.printf("[%d/%d] done, finishedTaskNum = %d\n", i, threadNum,
    // finishedTaskNum);
    // }
    // ApConnPool.close();
    // TpConnPool.close();
    // System.out.print("close done\n");
    // }
}