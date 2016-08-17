/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.route.algorithm;

import com.openddal.route.rule.ObjectNode;
import com.openddal.util.MurmurHash;
import com.openddal.value.Value;
import com.openddal.value.ValueLong;
import com.openddal.value.ValueTimestamp;

/**
 * @author jorgie.li
 */
public class HashBucketPartitioner extends CommonPartitioner {

    private static final int HASH_BUCKET_SIZE = 1024;

    private int[] count;
    private int[] length;
    private PartitionUtil partitionUtil;

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }

    @Override
    public void initialize(ObjectNode[] tableNodes) {
        super.initialize(tableNodes);
        partitionUtil = new PartitionUtil(HASH_BUCKET_SIZE, count, length);
    }

    @Override
    public Integer partition(Value value) {
        boolean isNull = checkNull(value);
        if (isNull) {
            return getDefaultNodeIndex();
        }
        byte[] bytes = toBytes(value);
        long hash64 = MurmurHash.hash64(bytes, bytes.length);
        return partitionUtil.partition(hash64);
    }
    
    
    private byte[] toBytes(Value value) {
        byte[] bytes;
        switch (value.getType()) {

        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
        case Value.FLOAT:
        case Value.DECIMAL:
        case Value.DOUBLE:
            long valueLong = value.getLong();
            bytes = ValueLong.get(valueLong).getBytes();
            break;
        case Value.DATE:
        case Value.TIME:
        case Value.TIMESTAMP:
            ValueTimestamp v = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
            long toLong = v.getTimestamp().getTime();
            bytes = ValueLong.get(toLong).getBytes();
            break;
        case Value.STRING:
        case Value.STRING_FIXED:
        case Value.STRING_IGNORECASE:
            String string = value.getString();
            bytes = string.getBytes();
        default:
            bytes = value.getBytes(); 
        }
        
        
        return bytes;
    }

}
