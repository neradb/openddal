package com.openddal.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Threads {

    /**
     * sleep等待, 单位为毫秒.
     */
    public static void sleep(long durationMillis) {
        try {
            Thread.sleep(durationMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * sleep等待.
     */
    public static void sleep(long duration, TimeUnit unit) {
        try {
            Thread.sleep(unit.toMillis(duration));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 按照ExecutorService JavaDoc示例代码编写的Graceful Shutdown方法. 先使用shutdown,
     * 停止接收新任务并尝试完成所有已存在任务. 如果超时, 则调用shutdownNow,
     * 取消在workQueue中Pending的任务,并中断所有阻塞函数. 如果仍然超時，則強制退出.
     * 另对在shutdown时线程本身被调用中断做了处理.
     */
    public static void shutdownGracefully(ExecutorService pool, int shutdownTimeout, int shutdownNowTimeout,
            TimeUnit timeUnit) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(shutdownTimeout, timeUnit)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(shutdownNowTimeout, timeUnit)) {
                    System.err.println("Pool did not terminated");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
    

    public static ThreadFactory newThreadFactory(String namePrefix) {
        return new CustomThreadFactory(namePrefix);
    }
    
    
    private static final class CustomThreadFactory implements ThreadFactory {
        private final static AtomicInteger index = new AtomicInteger(1);
        private final String prefix;
        private final boolean daemon;
        private final ThreadGroup group;

        public CustomThreadFactory(String prefix) {
            this(prefix, false);
        }

        private CustomThreadFactory(String prefix, boolean daemon) {
            SecurityManager sm = System.getSecurityManager();
            group = (sm != null) ? sm.getThreadGroup()
                    : Thread.currentThread().getThreadGroup();
            this.prefix = prefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            String name = prefix + "-" + index.getAndIncrement();
            Thread t = new Thread(group, r, name);
            t.setDaemon(daemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
