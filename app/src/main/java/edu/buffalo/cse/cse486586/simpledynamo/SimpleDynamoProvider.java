package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    private SharedPreferences sharedPreferenceStorage;
    private GlobalInfo globalInfo;
    private String KEY_FIELD = "key";
    private String VALUE_FIELD = "value";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {


        if(selectionArgs!=null)
        {
            SharedPreferences.Editor editor = sharedPreferenceStorage.edit();
            HashMap<String, String> keysInserted = globalInfo.getKeysInserted();

            editor.remove(selection);
            editor.commit();

            keysInserted.remove(selection);
        }
        else {

            try {
                String [] targetNodes = getCoordinatorAndReplicas(selection);
                MessageContainer messageToSend = new MessageContainer("Delete");
                messageToSend.setTargetNodes(targetNodes);
                messageToSend.setKey(selection);

                new ClientTask(globalInfo).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend);

                try {
                    messageToSend.getBlockQueue().take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }



            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return 0;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {


        String keyToInsert = (String) values.get(KEY_FIELD);
        String valueToInsert = (String) values.get(VALUE_FIELD);

        String [] valuesSplit = valueToInsert.split("#");

        if(valuesSplit.length == 1) {
            try {
                String[] targetNodes = getCoordinatorAndReplicas(keyToInsert);

                    //pass it to coordinator
                    MessageContainer messageToSend = new MessageContainer("Insert");
                    messageToSend.setTargetNodes(targetNodes);
                    messageToSend.setKeyValuePair(values);

                    long startTime = System.currentTimeMillis();
                    new ClientTask(globalInfo).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend);

                    try {
                        messageToSend.getBlockQueue().take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Log.v("Nee-INSERT","Insert successful for key - " + keyToInsert + " " + messageToSend.isInsertDone());
                    Log.e("Nee-INSERT","Time taken for insert - " + String.valueOf((System.currentTimeMillis()-startTime)));

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else
        {
            //insert here
            long timeStamp = System.currentTimeMillis();
            LinkedHashMap<String,String> keysInserted = globalInfo.getKeysInserted();

            if(valuesSplit.length==2) {
                //first time insert
                valueToInsert = valueToInsert + "#" + String.valueOf(timeStamp);
            }
            else if (valuesSplit.length==3)
            {
                //recovery case
                String existingValue = sharedPreferenceStorage.getString(keyToInsert,"defValue");

                if(!existingValue.equals("defValue"))
                {
                    long existingTimeStamp = Long.valueOf(existingValue.split("#")[2]);
                    long newTimeStamp = Long.valueOf(valuesSplit[2]);

                    if(newTimeStamp < existingTimeStamp)
                        valueToInsert = existingValue;
                }

            }

            SharedPreferences.Editor writer = sharedPreferenceStorage.edit();
            writer.putString(keyToInsert, valueToInsert);
            keysInserted.put(keyToInsert,"0");
            writer.commit();


            Log.v("Nee-INSERT","valueToInsert - " + valueToInsert);
        }


        return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        globalInfo = (GlobalInfo) getContext().getApplicationContext();
        sharedPreferenceStorage = getContext().getSharedPreferences("dbToStore", Context.MODE_MULTI_PROCESS);

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        String [] columnNames = {KEY_FIELD,VALUE_FIELD};
        MatrixCursor resultCursor = new MatrixCursor(columnNames);


        if(selection.equals("*"))
        {
            for(String key:globalInfo.getKeysInserted().keySet())
            {
                String valueExtracted = sharedPreferenceStorage.getString(key, "defValue");
                MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
                rowBuilder.add(key);
                rowBuilder.add(valueExtracted.split("#")[0]);
            }

            MessageContainer messageToSend = new MessageContainer("StarQuery");
            messageToSend.setMatCursor(resultCursor);
            new ClientTask(globalInfo).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend);

            try {
                messageToSend.getBlockQueue().take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        else if (selection.equals("@"))
        {

            for(String key:globalInfo.getKeysInserted().keySet())
            {
                String valueExtracted = sharedPreferenceStorage.getString(key, "defValue");
                MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
                rowBuilder.add(key);
                rowBuilder.add(valueExtracted.split("#")[0]);
            }

            Log.v("Nee-Query @","rows are - " + resultCursor.getCount());
        }
        else if(sortOrder!=null)
        {
            Log.v("Nee-InternalQuery","key - " + selection);
            String valueExtracted = sharedPreferenceStorage.getString(selection, "defValue");
            MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
            rowBuilder.add(selection);
            rowBuilder.add(valueExtracted);
        }
        else if (selection.equals("Recovery"))
        {
            for(String key:globalInfo.getKeysInserted().keySet())
            {
                String valueExtracted = sharedPreferenceStorage.getString(key, "defValue");
                String portNeeded = valueExtracted.split("#")[1];

                if(portNeeded.equals(selectionArgs[0]) || portNeeded.equals(selectionArgs[1])) {
                    MatrixCursor.RowBuilder rowBuilder = resultCursor.newRow();
                    rowBuilder.add(key);
                    rowBuilder.add(valueExtracted);
                }
            }
        }
        else
        {
            try {
                String[] targetNodes = getCoordinatorAndReplicas(selection);
                MessageContainer messageToSend = new MessageContainer("Query");
                messageToSend.setMatCursor(resultCursor);
                messageToSend.setTargetNodes(targetNodes);
                messageToSend.setKey(selection);

                long startTime = System.currentTimeMillis();
                Log.e("Nee-ClientQueryTime","Query for key - " + selection + " at - " + System.currentTimeMillis());
                new ClientTask(globalInfo).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend);

                String resultForQuery = null;

                try {
                    resultForQuery = messageToSend.getBlockQueue().take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if(resultForQuery!=null)
                {
                    resultCursor.moveToFirst();
                    resultForQuery = resultCursor.getString(resultCursor.getColumnIndex("value"));
                }
                Log.v("Nee-QUERY","Query successful for key -  "+ selection + " value is " + resultForQuery);

                Log.e("Nee-QUERY","Time taken for query - " + String.valueOf((System.currentTimeMillis()-startTime)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }

		return resultCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


	public String [] getCoordinatorAndReplicas(String key) throws NoSuchAlgorithmException {

        String hashedKey = genHash(key);

        ArrayList<Node> listOfNodes = globalInfo.getListOfNodes();

        int indexOfKey = -1;

        for(Node nodeToCompare:listOfNodes)
        {
            if(hashedKey.compareTo(nodeToCompare.getHashValue()) <= 0)
                break;

            indexOfKey++;
        }

        String [] targetNodes = new String[3];

        for(int i = 0;i<targetNodes.length;i++)
            targetNodes[i] = listOfNodes.get((indexOfKey+1+i)%listOfNodes.size()).getPortNo();

        return targetNodes;
	}
}
