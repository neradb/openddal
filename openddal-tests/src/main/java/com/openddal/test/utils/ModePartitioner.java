package com.openddal.test.utils;

import com.openddal.route.algorithm.CommonPartitioner;
import com.openddal.value.Value;

public class ModePartitioner extends CommonPartitioner {
    

    @Override
    public Integer partition(Value value) {
        boolean isNull = checkNull(value);
        if (isNull) {
            return getDefaultNodeIndex();
        }
        long v = value.getLong();
        return (int)(v % getTableNodes().length);
    }

}
