package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.MatrixCursor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Molu on 28/4/17.
 */

public class MessageContainer {

    public BlockingQueue<String> getBlockQueue() {
        return blockQueue;
    }

    public void setBlockQueue(BlockingQueue<String> blockQueue) {
        this.blockQueue = blockQueue;
    }

    private BlockingQueue<String> blockQueue = new ArrayBlockingQueue<String>(1);

    public MessageContainer(String header)
    {
        this.msgHeader = header;
    }

    public String[] getTargetNodes() {
        return targetNodes;
    }

    public ContentValues getKeyValuePair() {
        return keyValuePair;
    }

    public void setKeyValuePair(ContentValues keyValuePair) {
        this.keyValuePair = keyValuePair;
    }

    private ContentValues keyValuePair;

    private String key;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }


    public void setTargetNodes(String[] targetNodes) {
        this.targetNodes = targetNodes;
    }

    private String [] targetNodes;
    private boolean insertDone = false;

    public String getMsgHeader() {
        return msgHeader;
    }


    public boolean isInsertDone() {
        return insertDone;
    }

    public void setInsertDone(boolean insertDone) {
        this.insertDone = insertDone;
    }


    private String msgHeader;

    public MatrixCursor getMatCursor() {
        return matCursor;
    }

    public void setMatCursor(MatrixCursor matCursor) {
        this.matCursor = matCursor;
    }

    private MatrixCursor matCursor;
}
