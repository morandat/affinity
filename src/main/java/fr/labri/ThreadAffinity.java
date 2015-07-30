package fr.labri;


import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.jna.*;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.ptr.NativeLongByReference;

final public class ThreadAffinity {
    public static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("affinity.debug", "false"));
//    public static final int PID_T_SIZE = Integer.getInteger("affinity.pidt", Integer.SIZE / 8);
//    public static final int TID_T_SIZE = Integer.getInteger("affinity.tidt", Long.SIZE / 8);

    private static final ThreadAffinityProvider _provider;
    
    static public Core getCurrentCore() {
        return _provider.getCurrentCore();
    }
    static public Core[] getCores() {
        return _provider.getCores();
    }
    
    static public int nice(int nice) throws Exception {
        return _provider.nice(nice);
    }
    static public long getProcessAffinityMask() throws Exception {
        return _provider.getProcessAffinityMask();
    }
    static public void setProcessAffinityMask(long mask) throws Exception {
        _provider.setProcessAffinityMask(mask);
    }

    static public long getThreadID() {
        return _provider.getThreadID();
    }

    static public long getThreadAffinityMask() throws Exception {
        return _provider.getThreadAffinityMask();
    }
    static public long getThreadAffinityMask(NativeLong thread) throws Exception {
        return _provider.getThreadAffinityMask(thread);
    }

    static public void setThreadAffinityMask(long mask) throws Exception {
        _provider.setThreadAffinityMask(mask);
    }
    
    static public void setThreadAffinityMask(NativeLong thread, long mask) throws Exception {
        _provider.setThreadAffinityMask(thread, mask);
    }
    
    public abstract static class AbstractAffinityThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);

        protected final long _mask;
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public AbstractAffinityThreadFactory(long mask) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
            _mask = mask;
        }

        @Override
        final public Thread newThread(final Runnable r) {
            final int tn = threadNumber.getAndIncrement() - 1;
            Thread t = new Thread(group, new Runnable() {
                @Override
                public void run() {
                    setMask(tn);
                    if(r != null)
                        r.run();
                }
            }, namePrefix + tn, 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);

            return t;
        }
        
        final protected void setThreadMask(long mask) {
            try {
                if(DEBUG)
                    Utils.debug(this, "Trying to set mask of 0x", Long.toHexString(getThreadID()), " to 0x", Long.toHexString(mask), " (previously was 0x", Long.toHexString(getThreadAffinityMask()), ")");
                _provider.setThreadAffinityMask(mask);
                if(DEBUG)
                    Utils.debug(this, "Mask of 0x", Long.toHexString(getThreadID()), " set to 0x", Long.toHexString(getThreadAffinityMask()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        abstract protected void setMask(int tn);
    }
    
    final public static class AffinityThreadFactory extends AbstractAffinityThreadFactory {
        public AffinityThreadFactory() throws Exception {
            super(getThreadAffinityMask());
        }
        public AffinityThreadFactory(long mask) {
            super(mask);
        }
        @Override
        protected void setMask(int tn) {
            setThreadMask(_mask);
        }
    }
    
    final public static class SingleCPUThreadFactory extends AbstractAffinityThreadFactory{
        public SingleCPUThreadFactory() throws Exception {
            super(getThreadAffinityMask());
        }
        public SingleCPUThreadFactory(long mask) {
            super(mask);
        }
        
        @Override
        protected void setMask(int tn) {
            long threadMask = Utils.nthBit(_mask, tn % Long.bitCount(_mask));
            setThreadMask(threadMask);
        }
    }
    
    static abstract class ThreadAffinityProvider {
        abstract public Core getCurrentCore();
        abstract public Core[] getCores();
        abstract public int nice(int nice) throws Exception; // TODO replace by get/set priority

        abstract public long getProcessAffinityMask() throws Exception;
        abstract public void setProcessAffinityMask(long mask) throws Exception;

        abstract public long getThreadID();

        abstract public long getThreadAffinityMask() throws Exception;
        abstract public long getThreadAffinityMask(NativeLong threadId) throws Exception;

        abstract public void setThreadAffinityMask(long mask) throws Exception;
        abstract public void setThreadAffinityMask(NativeLong thread, long mask) throws Exception;
    }

    public static String getImplementationName() {
        return _provider.getClass().toString();
    }

    static private Core[] initCores(long n) {
        Core[] cores = new Core[Long.bitCount(n)];
        for(int i = 0; n > 0; i ++) {
            long currentBit = Long.lowestOneBit(n);
            cores[i] = new Core(currentBit);
            n &= ~currentBit;
        }
        
        return cores;
    }
    static {
        ThreadAffinityProvider provider;
        try {
            provider = new LinuxThreadAffinityProvider();
        } catch (Throwable e) {
            if(DEBUG)
                e.printStackTrace();
            try {
                provider = new MacOSXThreadAffinityProvider();
            } catch (Throwable ee) {
                if(DEBUG)
                    ee.printStackTrace();
                provider = new EmptyProvider();
            }
        }
        _provider = provider;
    }
    
    static private class PThreadAffinityProvider<L extends PThreadAffinityProvider.CLibrary> extends ThreadAffinityProvider {
        protected final L _cLibrary;
        private final PThreadLibrary _threadLibrary;
        protected final Core[] _cores;
        protected final NativeLong cpuMaskSize = new NativeLong(Native.LONG_SIZE);

        private PThreadAffinityProvider() throws Exception {
            _threadLibrary = (PThreadLibrary) Native.loadLibrary("pthread", PThreadLibrary.class);
            _cLibrary = initCLibrary();
            _cores = initCores(getProcessAffinityMask());
        }
        
        @SuppressWarnings("unchecked")
        protected L initCLibrary() {
            return (L) Native.loadLibrary("c", CLibrary.class);
        }
        
        private interface PThreadLibrary extends Library {
            public NativeLong pthread_self();
            // FIXME long is not acceptable for size_t nor tid
            public int pthread_getaffinity_np(final /*pthread_t*/ NativeLong tid, final /*size_t*/NativeLong cpusetsize, final PointerType cpuset) throws LastErrorException;
            public int pthread_setaffinity_np(final /*pthread_t*/ NativeLong tid, final /*size_t*/NativeLong cpusetsize, final PointerType cpuset) throws LastErrorException;
        }
        
        interface CLibrary extends Library {
            public int nice(final int increment) throws LastErrorException;
        }
        

        @Override
        public Core[] getCores() {
            return _cores;
        }

        @Override
        public long getThreadAffinityMask() throws Exception {
            return getThreadAffinityMask(_threadLibrary.pthread_self());
        }
        
        @Override
        public long getThreadAffinityMask(final NativeLong threadId) throws Exception {
            NativeLongByReference mask = new NativeLongByReference(); // FIXME
            final int ret = _threadLibrary.pthread_getaffinity_np(threadId, cpuMaskSize, mask);
            if (ret < 0)
                throw new LastErrorException("pthread_getaffinity_np( "+ threadId +", (" + cpuMaskSize + ") , &(" + mask + ") ) return " + ret);
            return mask.getValue().longValue();
        }

        @Override
        public void setThreadAffinityMask(final long mask) throws Exception {
            setThreadAffinityMask(_threadLibrary.pthread_self(), mask);
        }

        @Override
        public void setThreadAffinityMask(final NativeLong threadID, final long mask) throws Exception {
            LongByReference refmask = new LongByReference(mask);
            final int ret = _threadLibrary.pthread_setaffinity_np(threadID, cpuMaskSize, refmask);
            if (ret < 0)
                throw new LastErrorException("pthread_setaffinity_np( " + threadID + ", (" + cpuMaskSize + ") , &(" + refmask + ") ) return " + ret);
        }
        

        @Override
        public long getThreadID() {
            return _threadLibrary.pthread_self().longValue();
        }

        @Override
        public Core getCurrentCore() {
            int mask;
            try {
                mask = Long.bitCount(Long.lowestOneBit(getThreadAffinityMask()) - 1);
            } catch (Exception e) {
                mask = 0;
            }
            return _cores[mask];
        }

        @Override
        public int nice(final int increment) throws Exception {
            final int ret = _cLibrary.nice(increment);
            if (ret < 0) throw new LastErrorException("nice( " + increment + " ) return " + ret);
            return ret;
        }

        @Override
        public long getProcessAffinityMask() throws Exception {
            return getThreadAffinityMask();
        }

        @Override
        public void setProcessAffinityMask(long mask) throws Exception {
            setThreadAffinityMask(mask);
        }
    }
    
    static private class MacOSXThreadAffinityProvider extends PThreadAffinityProvider<PThreadAffinityProvider.CLibrary> {
        MacOSXThreadAffinityProvider() throws Exception {
            super();
        }
    }
    
    static private class LinuxThreadAffinityProvider extends PThreadAffinityProvider<LinuxThreadAffinityProvider.LinuxCLibrary> {
        interface LinuxCLibrary extends PThreadAffinityProvider.CLibrary {
            public int sched_getcpu() throws LastErrorException;
            // FIXME long is not acceptable for size_t
            public int sched_getaffinity(final /*pid_t*/ int pid, final /*size_t*/NativeLong cpusetsize, final PointerType cpuset) throws LastErrorException;
            public int sched_setaffinity(final /*pid_t*/  int pid, final /*size_t*/NativeLong cpusetsize, final PointerType cpuset) throws LastErrorException;
        }
        
        
        private LinuxThreadAffinityProvider() throws Exception {
            super();
        }

        @Override
        protected LinuxCLibrary initCLibrary() {
            return (LinuxCLibrary) Native.loadLibrary("c", LinuxCLibrary.class);
        }

        
        @Override
        public Core getCurrentCore() {
            final int cpuSequence = _cLibrary.sched_getcpu();
            return _cores[cpuSequence];
        }

        @Override
        public long getProcessAffinityMask() throws Exception {
            NativeLongByReference mask = new NativeLongByReference();
            final int ret = _cLibrary.sched_getaffinity(0, cpuMaskSize, mask);
            if (ret < 0)
                throw new LastErrorException("sched_getaffinity( 0, (" + cpuMaskSize + ") , &(" + mask + ") ) return " + ret);
            return mask.getValue().longValue();
        }

        @Override
        public void setProcessAffinityMask(long mask) throws Exception {
            final int ret = _cLibrary.sched_setaffinity(0, cpuMaskSize, new LongByReference(mask));
            if (ret < 0)
                throw new LastErrorException("sched_setaffinity( 0, (" + cpuMaskSize + ") , &(" + mask + ") ) return " + ret);
        }
    }

    private static class EmptyProvider extends ThreadAffinityProvider {
            Core[] _cores = initCores((1L << Runtime.getRuntime().availableProcessors()) - 1L);
            
            private long maxMask() {
                return (1L << _cores.length) - 1L;
            }
            @Override
            public Core[] getCores() {
                return _cores;
            }

            @Override
            public Core getCurrentCore() {
                return _cores[0];
            }

            @Override
            public int nice(int nice) {
                return 0;
            }

            @Override
            public long getProcessAffinityMask() {
                return maxMask();
            }

            @Override
            public long getThreadAffinityMask(NativeLong threadId) {
                return maxMask();
            }

            @Override
            public long getThreadAffinityMask() {
                return maxMask();
            }
            
            @Override
            public void setThreadAffinityMask(NativeLong thread, long mask) {
            }

            @Override
            public void setThreadAffinityMask(long mask) {
            }

            @Override
            public void setProcessAffinityMask(long mask) throws Exception {
            }
            @Override
            public long getThreadID() {
                return Thread.currentThread().getId();
            }
    }
    
    public static final class Core {
        private final long _mask;

        public Core(final long i) {
            _mask = i;
        }

        public long index() {
            return Long.bitCount(_mask - 1);
        }

        public void attachToCurrentThread() throws Exception {
            _provider.setThreadAffinityMask(_mask);
        }

//        public void attach(final Thread thread) throws Exception {
//            _provider.setThreadAffinityMask(thread, _mask);
//        }

        @Override
        public String toString() {
            return String.format("Core[#%d]", index());
        }
    }

}
