package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static java.security.AccessController.getContext;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        GlobalInfo globalInfo = (GlobalInfo) this.getApplication();

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        globalInfo.setMyPort(portStr);
        globalInfo.setmContentResolver(getContentResolver());
        globalInfo.setmUri(buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"));

		ServerSocket serverSocket;

		try {
			globalInfo.initializeListOfNodes();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

//		try {
//			Thread.sleep(50);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

        globalInfo.setRecoveryOngoing(true);

        long startTime = System.currentTimeMillis();

        SharedPreferences sharedPreferenceStorage = getSharedPreferences("dbToStore", Context.MODE_MULTI_PROCESS);

        HashMap<String,String> recoveryMap = globalInfo.getRecoveryMap();
        recoveryMap.clear();

		Map<String, ?> allEntries = sharedPreferenceStorage.getAll();

        for(Map.Entry<String,?> entry : allEntries.entrySet()){
            Log.d("Nee-map value",entry.getKey() + ": " + entry.getValue().toString());
            recoveryMap.put(entry.getKey(),entry.getValue().toString());
        }

        Log.e("Nee-ClientRecoveryTime","Time to load " + allEntries.size() + " keys into mem - " + String.valueOf(System.currentTimeMillis()-startTime));

		MessageContainer messageToSend = new MessageContainer("Recovery");
		new ClientTask(globalInfo).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend);

		try {
			serverSocket = new ServerSocket(10000);
			new ServerTask(globalInfo).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}
