package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static android.R.attr.key;

/**
 * Created by Molu on 28/4/17.
 */
public class ClientTask extends AsyncTask<MessageContainer, Void, Void> {

    private GlobalInfo globalInfo;
    private String KEY_FIELD = "key";
    private String VALUE_FIELD = "value";

    public ClientTask(GlobalInfo _gi)
    {
        globalInfo = _gi;
    }

    @Override
    protected Void doInBackground(MessageContainer... params) {

        MessageContainer msgContainer = params[0];

        if(msgContainer.getMsgHeader().equals("Delete"))
        {
            String [] targetNodes = msgContainer.getTargetNodes();
            String key = msgContainer.getKey();

            for(int i = 0; i<targetNodes.length;i++)
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(targetNodes[i]) * 2);

                    PrintWriter msgWriter = new PrintWriter(socket.getOutputStream());
                    BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

                    Log.v("Nee-Client Delete","sending delete to - " + targetNodes[i] + " key - " + key);
                    msgWriter.println("Delete");
                    msgWriter.flush();

                    msgWriter.println(key);
                    msgWriter.flush();

                    char rec_ack;

                    rec_ack = (char) buffReader.read();

                    if (rec_ack == 'A') {
                        socket.close();
                    } else {
                        Log.e("Nee-ClientTask Cleanup", "Broken port ack failed!");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            msgContainer.setInsertDone(true);

            try {
                msgContainer.getBlockQueue().put("Done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        if(msgContainer.getMsgHeader().equals("Insert"))
        {
            String [] targetNodes = msgContainer.getTargetNodes();
            ContentValues values = msgContainer.getKeyValuePair();

            String keyToInsert = (String) values.get(KEY_FIELD);
            String valueToInsert = (String) values.get(VALUE_FIELD);

            valueToInsert = valueToInsert+"#"+targetNodes[0];

            for(int i = 0; i<targetNodes.length;i++)
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(targetNodes[i]) * 2);

                    PrintWriter msgWriter = new PrintWriter(socket.getOutputStream());
                    BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

                    Log.v("Nee-Client insert","sending insert to - " + targetNodes[i] + " key - " + keyToInsert + " value - " + valueToInsert);
                    msgWriter.println("Insert");
                    msgWriter.flush();

                    msgWriter.println(keyToInsert);
                    msgWriter.flush();

                    msgWriter.println(valueToInsert);
                    msgWriter.flush();

                    char rec_ack;

                    rec_ack = (char) buffReader.read();

                    if (rec_ack == 'A') {
                        socket.close();
                    } else {
                        Log.e("Nee-ClientTask Cleanup", "Broken port ack failed! Port - " + targetNodes[i]);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            msgContainer.setInsertDone(true);

            try {
                msgContainer.getBlockQueue().put("Done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        if(msgContainer.getMsgHeader().equals("Query"))
        {
            String [] targetNodes = msgContainer.getTargetNodes();
            String key = msgContainer.getKey();
            String latestResponse = null;

            for(int i = 0; i<targetNodes.length;i++)
            {
                long startTime = System.currentTimeMillis();

                Log.d("Nee-ClientQueryTime","Asking " +targetNodes[i] +  " for Key - " + key + " Start - " + startTime);
                try {
                    Socket socket = new Socket();

                    try {
                        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(targetNodes[i]) * 2), 30);
                    }catch (SocketTimeoutException e)
                    {
                        Log.e("Nee-ClientQueryTime", "TOO LONG to connect!");
                        socket.close();
                        continue;
                    }

                    PrintWriter msgWriter = new PrintWriter(socket.getOutputStream());
                    BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

                    msgWriter.println("Query");
                    msgWriter.flush();

                    msgWriter.println(key);
                    msgWriter.flush();

                    if(i!=targetNodes.length-1)
                        socket.setSoTimeout(100);

                    try {
                        String response = buffReader.readLine();
                        Log.d("Nee-ClientQueryTime","Finish Total time - " + String.valueOf(System.currentTimeMillis()-startTime));

                        latestResponse = response;


                        msgWriter.write('A');
                        msgWriter.flush();

                        if(latestResponse!= null && !(latestResponse.equals("defValue")))
                            break;

                    } catch (SocketTimeoutException e)
                    {
                        Log.e("Nee-ClientQueryTime", "Query took TOO LONG!");
                        socket.close();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            MatrixCursor resultCursor = msgContainer.getMatCursor();
            MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
            rowBuilder.add(key);
            rowBuilder.add(latestResponse.split("#")[0]);

            try {
                msgContainer.getBlockQueue().put("Done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(msgContainer.getMsgHeader().equals("StarQuery"))
        {

            String [] targetNodes = new String [2];
            String [] ignoreNodes = new String [2];

            setTargetAndIgnoreNodes(targetNodes,ignoreNodes);

            MatrixCursor resultCursor = msgContainer.getMatCursor();

            for(int i = 0; i<targetNodes.length;i++)
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(targetNodes[i]) * 2);

                    Log.e("Nee-ClientStarQuery","Asking for * - " + targetNodes[i] + " ignoreNode - " + ignoreNodes[i]);
                    PrintWriter msgWriter = new PrintWriter(socket.getOutputStream());
                    BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

                    msgWriter.println("StarQuery");
                    msgWriter.flush();

                    long startTime = System.currentTimeMillis();

                    Log.d("Nee-ClientStarQueryTime","Start - " + startTime);

                    int noOfRows = Integer.parseInt(buffReader.readLine());

                    Log.e("Nee-ClientStarQuery","NofRows - " + String.valueOf(noOfRows));

                    while(noOfRows>0)
                    {
                        String blah = buffReader.readLine();
                        String [] keyValue = blah.split("@");

                        if(!globalInfo.getKeysInserted().containsKey(keyValue[0])) {
                            MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
                            rowBuilder.add(keyValue[0]);
                            rowBuilder.add(keyValue[1].split("#")[0]);
                        }
                        noOfRows--;
                    }


                    Log.d("Nee-ClientStarQueryTime","Finish Total time - " + String.valueOf(System.currentTimeMillis()-startTime));
                    msgWriter.write('A');
                    msgWriter.flush();

                    break;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                msgContainer.getBlockQueue().put("Done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        if(msgContainer.getMsgHeader().equals("Recovery"))
        {
            String [] targetNodes = new String[2];
            String [] ownerNodes = new String[2];

            ContentResolver mContentResolver = globalInfo.getmContentResolver();
            Uri mUri = globalInfo.getmUri();
            HashMap<String,String> recoveryMap = globalInfo.getRecoveryMap();
            HashMap<String, String> keysInserted = globalInfo.getKeysInserted();
            setTargetAndOwnerNodes(targetNodes,ownerNodes);

            long startTime = System.currentTimeMillis();

            Log.d("Nee-ClientRecoveryTime","Start - " + startTime);
            for(int i = 0; i<targetNodes.length;i++) {

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(targetNodes[i]) * 2);

                    PrintWriter msgWriter = new PrintWriter(socket.getOutputStream());
                    BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

                    Log.v("Nee-ClientRecovery","Connecting to "+ targetNodes[i] + " owner nodes " + ownerNodes[i]);


                    msgWriter.println("Recovery");
                    msgWriter.flush();

                    msgWriter.println(ownerNodes[i]);
                    msgWriter.flush();

                    String entries = buffReader.readLine();


                    if(entries == null || entries.equals("CloseConnection"))
                    {
                        Log.e("Nee-ClientRecovery","ZERO ENTRIES!");
                        socket.close();
                        continue;
                    }

                    int noOfEntries = Integer.parseInt(entries);

                    Log.e("Nee-ClientRecovery","NoOfEntries - " + String.valueOf(noOfEntries));
                    ContentValues [] conVal = new ContentValues[noOfEntries];

                    for(int j = 0;j<conVal.length;j++)
                    {
                        String blah = buffReader.readLine();
                        String [] keyValue = blah.split("@");

                        conVal[j] = new ContentValues();
                        conVal[j].put(KEY_FIELD, keyValue[0]);
                        conVal[j].put(VALUE_FIELD, keyValue[1]);
                    }

                    msgWriter.write('A');
                    msgWriter.flush();

                    long writeTime = System.currentTimeMillis();

                    Log.d("Nee-ClientRecoveryTime","Start Writing - " + writeTime);

                    int keyWritten = 0;
                    for(int j = 0;j<conVal.length;j++)
                    {
                        String key = conVal[j].getAsString(KEY_FIELD);
                        String value = conVal[j].getAsString(VALUE_FIELD);

                        String existingValue = recoveryMap.get(key);


                        if(existingValue!=null && existingValue.split("#")[0].equals(value.split("#")[0]))
                        {
                            keysInserted.put(key,"0");
                        }
                        else {
                            mContentResolver.insert(mUri, conVal[j]);
                            keyWritten++;
                        }

                        recoveryMap.remove(key);
                    }

                    Log.d("Nee-ClientRecoveryTime","Total write time for " + keyWritten + " keys - " + String.valueOf(System.currentTimeMillis()-writeTime));


                } catch (IOException e) {
                    Log.e("Nee-ClientRecovery","could not connect!");
                }

            }

            Set<String> keySet = recoveryMap.keySet();

            for(String eachKey:keySet)
            {
                mContentResolver.delete(mUri,eachKey,new String[] {"Neerad"});
            }

            globalInfo.setRecoveryOngoing(false);

            Log.d("Nee-ClientRecoveryTime","Finish Total time - " + String.valueOf(System.currentTimeMillis()-startTime));

        }
        return null;
    }


    //For recovery purpose
    private void setTargetAndOwnerNodes(String[] targetNodes, String[] ownerNodes) {

        String myPort = globalInfo.getMyPort();
        ArrayList<Node> listOfNodes = globalInfo.getListOfNodes();

        try {
            Node myNode = new Node(myPort, SimpleDynamoProvider.genHash(myPort));
            targetNodes[0] = listOfNodes.get((listOfNodes.indexOf(myNode)+1)%listOfNodes.size()).getPortNo();
            String pred = listOfNodes.get((listOfNodes.indexOf(myNode)+4)%listOfNodes.size()).getPortNo();
            ownerNodes[0] = myPort+"#"+pred;

            String secondPred = listOfNodes.get((listOfNodes.indexOf(myNode)+3)%listOfNodes.size()).getPortNo();

            targetNodes[1] = pred;
            ownerNodes[1] = secondPred + "#" + "neerad";

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


    }


    private void setTargetAndIgnoreNodes(String[] targetNodes, String[] ignoreNodes) {

        ArrayList<Node> listOfNodes = globalInfo.getListOfNodes();
        Node newNode = null;

        try {
            newNode = new Node(globalInfo.getMyPort(), SimpleDynamoProvider.genHash(globalInfo.getMyPort()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        targetNodes[0] = listOfNodes.get((listOfNodes.indexOf(newNode)+2)%listOfNodes.size()).getPortNo();
        ignoreNodes[0] = globalInfo.getMyPort();

        targetNodes[1] = listOfNodes.get((listOfNodes.indexOf(newNode)+3)%listOfNodes.size()).getPortNo();
        ignoreNodes[1] = targetNodes[1];

    }
}
