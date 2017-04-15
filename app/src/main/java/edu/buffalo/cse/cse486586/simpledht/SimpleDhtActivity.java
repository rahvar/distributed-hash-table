package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {



    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        //findViewById(R.id.button3).setOnClickListener(
         //       new OnTestClickListener(tv, getContentResolver()));



        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

               ContentResolver contentProvider = getContentResolver();
                Uri gUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                ContentValues cv = new ContentValues();


                cv.put("key", "123214");
                cv.put("value", "three");

                contentProvider.insert(gUri,cv);
                ContentValues cv2 = new ContentValues();
                cv2.put("key", "523214");
                cv2.put("value", "four");

                contentProvider.insert(gUri,cv2);

                contentProvider.query(gUri,null,"*",null,null);



            }
        });




        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                ContentResolver contentProvider = getContentResolver();
                Uri gUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                //ContentValues cv = new ContentValues();




                contentProvider.query(gUri,null,"*",null,null);
                contentProvider.delete(gUri,"123214",null);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                contentProvider.query(gUri,null,"*",null,null);

            }
        });


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
