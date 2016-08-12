package com.openddal.server.mysql;

import java.io.Closeable;

import io.netty.buffer.ByteBuf;

public class MysqlPreparedStatement implements Closeable {

    public int[] types;
    PreparedStatement script;
    Heap heap = new FlexibleHeap();
    VaporizingRow row;
    
    public MysqlPreparedStatement(Orca orca, PreparedStatement script) {
        super();
        this.script = script;
        this.row = new VaporizingRow(heap, script.getParameterCount()-1);
    }

    public int getId() {
        return script.hashCode();
    }

    public int getParameterCount() {
        return this.script.getParameterCount();
    }

	public Heap getHeap() {
		return this.heap;
	}

	public void setParam(int paramId, long pValue) {
    	row.setFieldAddress(paramId, pValue);
	}

	public Parameters getParams() {
		return new FishParameters(this.row);
	}

	public Object run(Session session) {
		FishParameters params = new FishParameters(row);
		Object result = this.script.run(session, params);
		return result;
	}

	@Override
	public void close() {
		if (this.heap != null) {
			this.heap.free();
		}
		this.row = null;
		this.heap = null;
	}

	public void clear() {
		this.heap.reset(0);
		this.row = new VaporizingRow(heap, getParameterCount()-1);
	}

	public void setParam(int paramId, ByteBuf content) {
	}

	public ByteBuf getLongData(int i) {
		return null;
	}
	
	public String getSql() {
		return this.script.getSql();
	}
}