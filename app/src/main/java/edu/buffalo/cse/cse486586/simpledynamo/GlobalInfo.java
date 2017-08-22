package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Application;
import android.content.ContentResolver;
import android.net.Uri;

import java.lang.reflect.Array;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by Molu on 28/4/17.
 */
public class GlobalInfo extends Application {

    public boolean isRecoveryOngoing() {
        return recoveryOngoing;
    }

    public void setRecoveryOngoing(boolean recoveryOngoing) {
        this.recoveryOngoing = recoveryOngoing;
    }

    public HashMap<String, String> getRecoveryMap() {
        return recoveryMap;
    }

    public void setRecoveryMap(HashMap<String, String> recoveryMap) {
        this.recoveryMap = recoveryMap;
    }

    private HashMap<String,String> recoveryMap = new HashMap<String, String>();

    private boolean recoveryOngoing = false;

    public ArrayList<Node> getListOfNodes() {
        return listOfNodes;
    }

    private ArrayList<Node> listOfNodes = new ArrayList<Node>();

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    private String myPort;

    public final static int STAR_SLEEP_TIME = 1000;

    public void initializeListOfNodes() throws NoSuchAlgorithmException {

        listOfNodes.add(new Node("5554",SimpleDynamoProvider.genHash("5554")));
        listOfNodes.add(new Node("5556",SimpleDynamoProvider.genHash("5556")));
        listOfNodes.add(new Node("5558",SimpleDynamoProvider.genHash("5558")));
        listOfNodes.add(new Node("5560",SimpleDynamoProvider.genHash("5560")));
        listOfNodes.add(new Node("5562",SimpleDynamoProvider.genHash("5562")));

        Collections.sort(listOfNodes);
    }

    public void setmContentResolver(ContentResolver mContentResolver) {
        this.mContentResolver = mContentResolver;
    }

    public ContentResolver getmContentResolver() {
        return mContentResolver;
    }

    private static ContentResolver mContentResolver;

    public Uri getmUri() {
        return mUri;
    }

    public void setmUri(Uri mUri) {
        this.mUri = mUri;
    }

    private Uri mUri;

    public LinkedHashMap<String,String> getKeysInserted() {
        return keysInserted;
    }

    public void setKeysInserted(LinkedHashMap<String,String> keysInserted) {
        this.keysInserted = keysInserted;
    }

    private LinkedHashMap<String,String> keysInserted = new LinkedHashMap<String, String>();
}
