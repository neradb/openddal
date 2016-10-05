package com.openddal.repo;

import java.util.Properties;
import java.util.Random;

import com.openddal.config.SequenceRule;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;

public class SnowflakeGenerator extends Sequence {

    private long currentValue;
    private IdWorker idWorker;

    public SnowflakeGenerator(Schema schema, String name, SequenceRule config) {
        super(schema, name, 1, 1);
        configure(config.getProperties());
    }

    public void configure(Properties params) {
        int sequenceId = TableHiLoGenerator.getIntProperty(params, "sequenceId", -1);
        int instanceId = TableHiLoGenerator.getIntProperty(params, "instanceId", -1);
        if (sequenceId < 0 || sequenceId > IdWorker.maxWorkerId) {
            throw DbException.getInvalidValueException("sequenceId", sequenceId);
        }
        if (instanceId < 0 || instanceId > IdWorker.maxDatacenterId) {
            throw DbException.getInvalidValueException("instanceId", instanceId);
        }
        this.idWorker = new IdWorker(sequenceId, instanceId, 0);
        
    }


    @Override
    public synchronized long getNext(Session session) {
        long id = idWorker.getId();
        currentValue = id;
        return id;
    }

    @Override
    public synchronized long getCurrentValue() {
        if (currentValue < 1) {
            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                    "sequence " + getName() + ".currval is not yet defined in this session");
        }
        return currentValue;
    }

    public static class IdWorker {

        private final long workerId;
        private final long datacenterId;
        private final long idepoch;

        private static final long workerIdBits = 5L;
        private static final long datacenterIdBits = 5L;
        private static final long maxWorkerId = -1L ^ (-1L << workerIdBits);
        private static final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);

        private static final long sequenceBits = 12L;
        private static final long workerIdShift = sequenceBits;
        private static final long datacenterIdShift = sequenceBits + workerIdBits;
        private static final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
        private static final long sequenceMask = -1L ^ (-1L << sequenceBits);

        private long lastTimestamp = -1L;
        private long sequence;
        private static final Random r = new Random();

        public IdWorker() {
            this(1468947930946L);
        }

        public IdWorker(long idepoch) {
            this(r.nextInt((int) maxWorkerId), r.nextInt((int) maxDatacenterId), 0, idepoch);
        }

        public IdWorker(long workerId, long datacenterId, long sequence) {
            this(workerId, datacenterId, sequence, 1468947930946L);
        }

        //
        public IdWorker(long workerId, long datacenterId, long sequence, long idepoch) {
            this.workerId = workerId;
            this.datacenterId = datacenterId;
            this.sequence = sequence;
            this.idepoch = idepoch;
            if (workerId < 0 || workerId > maxWorkerId) {
                throw new IllegalArgumentException("workerId is illegal: " + workerId);
            }
            if (datacenterId < 0 || datacenterId > maxDatacenterId) {
                throw new IllegalArgumentException("datacenterId is illegal: " + workerId);
            }
            if (idepoch >= System.currentTimeMillis()) {
                throw new IllegalArgumentException("idepoch is illegal: " + idepoch);
            }
        }

        public long getDatacenterId() {
            return datacenterId;
        }

        public long getWorkerId() {
            return workerId;
        }

        public long getTime() {
            return System.currentTimeMillis();
        }

        public long getId() {
            long id = nextId();
            return id;
        }

        private synchronized long nextId() {
            long timestamp = timeGen();
            if (timestamp < lastTimestamp) {
                throw new IllegalStateException("Clock moved backwards.");
            }
            if (lastTimestamp == timestamp) {
                sequence = (sequence + 1) & sequenceMask;
                if (sequence == 0) {
                    timestamp = tilNextMillis(lastTimestamp);
                }
            } else {
                sequence = 0;
            }
            lastTimestamp = timestamp;
            long id = ((timestamp - idepoch) << timestampLeftShift)//
                    | (datacenterId << datacenterIdShift)//
                    | (workerId << workerIdShift)//
                    | sequence;
            return id;
        }

        /**
         * get the timestamp (millis second) of id
         * 
         * @param id the nextId
         * @return the timestamp of id
         */
        public long getIdTimestamp(long id) {
            return idepoch + (id >> timestampLeftShift);
        }

        private long tilNextMillis(long lastTimestamp) {
            long timestamp = timeGen();
            while (timestamp <= lastTimestamp) {
                timestamp = timeGen();
            }
            return timestamp;
        }

        private long timeGen() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("IdWorker{");
            sb.append("workerId=").append(workerId);
            sb.append(", datacenterId=").append(datacenterId);
            sb.append(", idepoch=").append(idepoch);
            sb.append(", lastTimestamp=").append(lastTimestamp);
            sb.append(", sequence=").append(sequence);
            sb.append('}');
            return sb.toString();
        }
    }

}
