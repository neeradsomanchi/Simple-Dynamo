package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Molu on 28/4/17.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    GlobalInfo globalInfo;
    String VALUE_FIELD = "value";

    public ServerTask(GlobalInfo _gi) {
        globalInfo = _gi;
    }

    @Override
    protected Void doInBackground(ServerSocket... params) {

        ServerSocket serverSocket = params[0];
        int count = 0;

        while(count < 99999999)
        {

            try {
                Socket receiverSocket = serverSocket.accept();

                BufferedReader buffReader = new BufferedReader(new InputStreamReader(receiverSocket.getInputStream(), "UTF-8"));

                PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(receiverSocket.getOutputStream(), "UTF-8"));

                String header = buffReader.readLine();

                if(header.equals("Insert"))
                {
                    String keyToInsert = buffReader.readLine();
                    String valueToInsert = buffReader.readLine();

                    ContentValues cv = new ContentValues();

                    cv.put("key", keyToInsert);
                    cv.put("value", valueToInsert);

                    ContentResolver mContentResolver = globalInfo.getmContentResolver();
                    mContentResolver.insert(globalInfo.getmUri(), cv);

                    printWriter.write('A');
                    printWriter.flush();
                }

                if(header.equals("Delete"))
                {
                    String key = buffReader.readLine();
                    ContentResolver mContentResolver = globalInfo.getmContentResolver();

                    mContentResolver.delete(globalInfo.getmUri(),key,new String[] {"Neerad"});

                    printWriter.write('A');
                    printWriter.flush();
                }

                if(header.equals("Query"))
                {
                    String key = buffReader.readLine();

                    ContentResolver mContentResolver = globalInfo.getmContentResolver();

                    String valueToReturn;

                    Log.d("Nee-ServerTask","Query for key - " + key);
                    if(globalInfo.isRecoveryOngoing())
                    {
                        valueToReturn = "defValue";
                    }
                    else {
                        Cursor resultCursor = mContentResolver.query(globalInfo.getmUri(), null,
                                key, null, "InternalQuery");

                        resultCursor.moveToFirst();

                        valueToReturn = resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD));

                    }
                    printWriter.println(valueToReturn);
                    printWriter.flush();

                    char rec_ack;

                    rec_ack = (char) buffReader.read();

                    if (rec_ack == 'A') {
                        receiverSocket.close();
                    } else {
                        Log.e("Nee-ClientTask Cleanup", "Broken port ack failed!");
                    }

                }

                if(header.equals("StarQuery"))
                {
                    printWriter.println(globalInfo.getKeysInserted().size());
                    printWriter.flush();

                    ContentResolver mContentResolver = globalInfo.getmContentResolver();

                    Cursor resultCursor = mContentResolver.query(globalInfo.getmUri(), null,
                            "@", null, null);

                    resultCursor.moveToFirst();



                    for(int i = 0;i<resultCursor.getCount();i++)
                    {
                        String valueToSend = resultCursor.getString(resultCursor.getColumnIndex("key"));
                        valueToSend = valueToSend + "@" + resultCursor.getString(resultCursor.getColumnIndex("value"));

//                        Log.e("Nee-ServerStar","valueTosend - " + valueToSend);
                        printWriter.println(valueToSend);
                        printWriter.flush();

                        resultCursor.moveToNext();
                    }


                    char rec_ack = (char) buffReader.read();

                    if (rec_ack == 'A') {
                        receiverSocket.close();
                    } else {
                        Log.e("Nee-ClientTask Cleanup", "Broken port ack failed!");
                    }
                }

                if(header.equals("Recovery"))
                {
                    String [] selectionArgs = buffReader.readLine().split("#");

                    ContentResolver mContentResolver = globalInfo.getmContentResolver();
                    Cursor resultCursor = mContentResolver.query(globalInfo.getmUri(), null,
                            "Recovery", selectionArgs, null);

                    Log.v("Nee-ServerRecovery","resultCursor count - " + String.valueOf(resultCursor.getCount()));

                    if(resultCursor.getCount()==0)
                    {
                        Log.v("Nee-ServerRecovery","Closing connection");
                        printWriter.println("CloseConnection");
                        printWriter.flush();
                        receiverSocket.close();
                        continue;
                    }

                    Log.e("Nee-ServerRecovery","NoContinue");
                    printWriter.println(resultCursor.getCount());
                    printWriter.flush();

                    resultCursor.moveToFirst();

                    for(int i = 0;i<resultCursor.getCount();i++)
                    {
                        String valueToSend = resultCursor.getString(resultCursor.getColumnIndex("key"));
                        valueToSend = valueToSend + "@" + resultCursor.getString(resultCursor.getColumnIndex("value"));

//                        Log.e("Nee-ServerRecovery","valueTosend - " + valueToSend);
                        printWriter.println(valueToSend);
                        printWriter.flush();

                        resultCursor.moveToNext();
                    }

                    char rec_ack = (char) buffReader.read();

                    if (rec_ack == 'A') {
                        receiverSocket.close();
                    } else {
                        Log.e("Nee-ServerRecovery", "Broken port ack failed!");
                    }

                }


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
