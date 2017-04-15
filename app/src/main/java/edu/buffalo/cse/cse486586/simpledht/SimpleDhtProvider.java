package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {


    String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    ArrayList<String> portList = new ArrayList<String>(Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));
    static final int SERVER_PORT = 10000;

    ArrayList<String> pList = new ArrayList<String>(Arrays.asList("5554","5556","5558","5560","5562"));
    ArrayList<Node> nodeList = new ArrayList<Node>();
    String predNode = "";
    String succNode = "";

    String predNodeId="";
    String succNodeId="";

    String cNode="";
    String cNodeId="";



    ContentResolver contentProvider;
    Uri gUri;
    BlockingQueue<String> bq = new ArrayBlockingQueue<String>(10);

    /**
     * ATTENTION: This was auto-generated to implement the App Indexing API.
     * See https://g.co/AppIndexing/AndroidStudio for more information.
     */


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub



        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        TAG += portStr;
        Log.d(TAG,"Content provider created");
        //emID = portStr;
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        //TAG = TAG + emID;

        //String node_id = "";
        contentProvider = getContext().getContentResolver();
        gUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");



        cNode = portStr;

        try {
            cNodeId = genHash(cNode);

            String s = "";
            for(String p:pList){
                s+= p +":"+ genHash(p)+"--";
            }
            Log.d(TAG,"PortHashes:"+s);

        } catch (NoSuchAlgorithmException e) {
            Log.d(TAG,e+"");
        }


        if(myPort.equals(REMOTE_PORT0)){

            Node n = new Node(cNode,cNodeId);
            nodeList.add(n);
            Log.d(TAG,"NodeList start check:"+nodeList.size());

        }
        if(!myPort.equals(REMOTE_PORT0)) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);


            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            //new DeliverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            //return;
        }

        String joinmsg = "NodeJoined:"+portStr;

        if(!myPort.equals(REMOTE_PORT0))
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinmsg, myPort);
        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader reader;
            //serverSocket.toString();

            String data = "";

            try {

                while (true) {

                    Socket clientSocket = serverSocket.accept();


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));

                    data = in.readLine();

                    DataOutputStream outStream = new DataOutputStream(clientSocket.getOutputStream());

                    if(data.startsWith("NodeJoin")){

                        Log.d(TAG,"ReceivedJoinRequest:"+data);

                        String[] pStr = data.split(":");
                        String newNodePort  = pStr[1];

                        String newNodeId = genHash(newNodePort);

                        Node newNode = new Node(newNodePort,newNodeId);

                        Log.d(TAG,"Checking Node List:"+nodeList.size());
                        Node currNode = nodeList.get(0);


                        if(currNode.getSucc()==null && currNode.getPred()==null){
                            //Log.d(TAG,"Second Node Joining");
                            currNode.setSucc(newNode);
                            currNode.setPred(newNode);
                            newNode.setSucc(currNode);
                            newNode.setPred(currNode);
                        }
                        else if(currNode.getNodeId().compareTo(newNode.getNodeId())<0 ){
                            //Log.d(TAG,"GreaterThan");
                            Node succ = currNode.getSucc();
                            while(succ.getNodeId().compareTo(newNode.getNodeId())<0){
                                succ = succ.getSucc();
                                if(succ.getNodeId().compareTo(succ.getPred().getNodeId())<0)
                                    break;
                            }
                            Node predNode = succ.getPred();
                            predNode.setSucc(newNode);
                            succ.setPred(newNode);

                            newNode.setPred(predNode);
                            newNode.setSucc(succ);

                        }
                        else{
                            //Log.d(TAG,"LesserThan");

                            Node pred= currNode.getPred();
                            Log.d(TAG,"Checking:"+pred.getNodeId()+":"+newNode.getNodeId());
                            while(pred.getNodeId().compareTo(newNode.getNodeId())>0){
                                pred = pred.getPred();
                                Log.d(TAG,"Checking:"+pred.getNodeId()+":"+newNode.getNodeId());
                                if(pred.getNodeId().compareTo(pred.getSucc().getNodeId())>0 && newNode.getNodeId().compareTo(pred.getNodeId())<0   )
                                    break;
                            }

                            Log.d(TAG,"CheckingOut:"+pred.getNodeId()+":"+newNode.getNodeId());
                            Node succNode= pred.getSucc();
                            succNode.setPred(newNode);
                            pred.setSucc(newNode);

                            newNode.setPred(pred);
                            newNode.setSucc(succNode);

                        }

                        nodeList.add(newNode);

                        String p="";
                        for(Node nd:nodeList){
                            p+= nd.getPortStr()+"-" +nd.getPred().getPortStr()+"-"+ nd.getSucc().getPortStr()+":::";

                        }
                        Log.d(TAG,"CurrentJoinStatus:"+p);
                        outStream.writeBytes("OK_"+p+"\n");
                    }
                    else if(data.startsWith("Info")){

                        String[] info = data.split("-");
                        predNode = info[1];
                        succNode = info[2];

                        Log.d(TAG,"ReceivedNodeInfo:"+predNode+"-"+succNode);

                        predNodeId = genHash(predNode);
                        succNodeId = genHash(succNode);
                        outStream.writeBytes("Done\n");
                    }


                    else if(data.startsWith("Route")){

                        String[] routeInfo = data.split(":");

                        ContentValues cv = new ContentValues();

                        String cvKey = routeInfo[1];
                        String cvValue = routeInfo[2];

                        Log.d(TAG,"FirstRoute:"+cvKey+":"+cvValue);
                        cv.put("key", cvKey);
                        cv.put("value", cvValue);
                        contentProvider.insert(gUri, cv);

                        outStream.writeBytes("RouteDone\n");

                    }

                    else if(data.startsWith(("Query"))){
                        String[] queryInfo = data.split(":");
                        String queryKey = queryInfo[1];
                        String senderInfo = queryInfo[2];

                        Log.d(TAG,"ReceivedQuery:"+queryKey);
                        String query = "Searching:"+queryKey+":"+senderInfo;
                        contentProvider.query(gUri,null,query,null,null);


                        outStream.writeBytes("QueryDone\n");
                    }

                    else if(data.startsWith("FoundKey")){

                        String[] fKeyInfo = data.split(":");

                        String key = fKeyInfo[1];
                        String value = fKeyInfo[2];

                        String fin = "Found:"+key+":"+value;



                        String  keyVal = key+":"+value;

                        bq.put(keyVal);
                        Log.d(TAG,"FinalQDone:"+fin+"Size:"+bq.size());
                        //contentProvider.query(gUri,null,fin,null,null);

                        outStream.writeBytes("FoundKeyDone\n");
                    }
                    else if(data.startsWith("FoundStar")){
                        Log.d(TAG,"StarRequest");

                        String sender = data.split(":")[3];
                        String out = "";
                        Cursor cursor = contentProvider.query(gUri,null,"@",null,null);
                        if (cursor.moveToFirst()){
                            do{
                                String k1 = cursor.getString(cursor.getColumnIndex("key"));
                                String v1 = cursor.getString(cursor.getColumnIndex("value"));
                                out+=k1+"-"+v1+":";

                                // do what ever you want here
                            }while(cursor.moveToNext());
                        }
                        cursor.close();

                        Log.d(TAG,"RetrievedData::"+sender+":::"+out);

                        if(out.isEmpty()){
                            out="*";
                        }

                        outStream.writeBytes("FoundStarDone::"+succNode+"::"+out+"\n");


                    }
                    else if(data.startsWith("Delete")){

                        contentProvider.delete(gUri,"@",null);
                        outStream.writeBytes("DeleteDone:"+succNode+"\n");
                    }
                    else if(data.startsWith("DelKey")){
                        String[] inf = data.split(":");
                        Log.d(TAG,"Server delete key: "+inf[1].trim());
                        contentProvider.delete(gUri,inf[1].trim(),null);
                        outStream.writeBytes("DelKeyAck\n");
                    }
                }

            } catch (Exception e) {
                Log.e(TAG, e + "");
            }


            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0]+"\n";

            String remotePort;
            String nodeInfo="";
            try {


                if(msgToSend.startsWith("NodeJoin")){


                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT0));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));



                    outStream.writeBytes(msgToSend);

                    outStream.flush();

                    Log.d(TAG,"SentJoinRequest:"+msgToSend);
                    String ack = in.readLine();


                    //Log.e(TAG, ack + remotePort+" - checking ACK");
                    if (ack.startsWith("OK")) {

                        Log.d(TAG,"AckNodeJoined"+ack);
                        nodeInfo = ack.split("_")[1];
                        socket.close();
                    }
                }

            }catch (Exception e){
                Log.d(TAG,e+"");
            }




            if(msgToSend.startsWith("NodeJoin")) {

                Log.d(TAG,nodeInfo);
                String[] connInfo = nodeInfo.split(":::");
                Log.d(TAG,"ConnSize:"+connInfo.length);

                try {


                    for (String conn : connInfo) {

                        Log.d(TAG, conn);


                        String[] portInfo = conn.split("-");
                        remotePort = Integer.toString(Integer.parseInt(portInfo[0]) * 2);


                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));


                        String sending = "Info-" + portInfo[1] + "-" + portInfo[2] + "\n";
                        outStream.writeBytes(sending);

                        outStream.flush();

                        Log.d(TAG, "SentNodeInfo:" + portInfo[0] + "-Pred:" + portInfo[1] + "-Succ:" + portInfo[2]);
                        String ack = in.readLine();


                        Log.e(TAG, ack + remotePort + " - checking ACK");
                        if (ack.startsWith("Done")) {

                            socket.close();
                        }

                    }
                } catch (Exception e) {
                    Log.d(TAG,"JoiningException:" +e);
                }
            }

            if(msgToSend.startsWith("Route")){
                try{



                    Log.d(TAG,"AboutToRoute: "+msgToSend);

                    remotePort = Integer.toString(Integer.parseInt(succNode) * 2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    outStream.writeBytes(msgToSend);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String ack = in.readLine();
                    if(ack.startsWith("RouteDone")){
                        socket.close();
                    }
                }
                catch (Exception e){
                    Log.d(TAG,"RoutingException:"+e);
                }



            }

            if(msgToSend.startsWith("Query")){

                try{



                    Log.d(TAG,"AboutToQuery: "+msgToSend+succNode);

                    remotePort = Integer.toString(Integer.parseInt(succNode) * 2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    outStream.writeBytes(msgToSend);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String ack = in.readLine();
                    if(ack.startsWith("QueryDone")){
                        socket.close();
                    }
                }
                catch (Exception e){
                    Log.d(TAG,"QueryingException:"+e);
                }

            }


            if(msgToSend.startsWith("FoundKey")){
                try{



                    Log.d(TAG,"FoundKeyClient: "+msgToSend);

                    String[] data = msgToSend.split(":");
                    remotePort = Integer.toString(Integer.parseInt(data[3].trim()) * 2);
                    Log.d(TAG,"FoundKeyClient:"+remotePort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    outStream.writeBytes(msgToSend);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String ack = in.readLine();
                    if(ack.startsWith("FoundKeyDone")){
                        socket.close();
                    }
                }
                catch (Exception e){
                    Log.d(TAG,"FoundKeyException:"+e);
                }
            }

            if(msgToSend.startsWith("FoundStar")){
                Log.d(TAG,"Star"+cNode);
                try{

                String[] data = msgToSend.split(":");

                String sendPort = data[3].trim();
                String allVals = "";
                while(!cNode.equals(sendPort)){

                    remotePort = Integer.toString(Integer.parseInt(sendPort) * 2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    outStream.writeBytes(msgToSend);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String ack = in.readLine();
                    if(ack.startsWith("FoundStarDone")){

                        String[] inf = ack.split("::");
                        sendPort = inf[1];

                        Log.d(TAG,"NextPort:"+sendPort);

                        if(!inf[2].trim().equals("*"))
                            allVals += inf[2].trim();

                        socket.close();
                    }


                }
                    Log.d(TAG,"ReceivedALLData"+allVals);
                    bq.put(allVals);

                }catch (Exception e){
                    Log.d(TAG,"StarExc"+e);
                }



            }
            if(msgToSend.startsWith("DelKey")){

                try{

                    Log.d(TAG,"AboutToDelete: "+msgToSend+succNode);

                    remotePort = Integer.toString(Integer.parseInt(succNode) * 2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    outStream.writeBytes(msgToSend);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String ack = in.readLine();
                    if(ack.startsWith("DelKeyAck")){
                        socket.close();
                    }
                }
                catch (Exception e){
                    Log.d(TAG,"DekKeyException:"+e);
                }


            }

            if(msgToSend.startsWith("Delete")){

                try{

                    String[] data = msgToSend.split(":");

                    String sendPort = data[1].trim();
                    while(!cNode.equals(sendPort)){

                        remotePort = Integer.toString(Integer.parseInt(sendPort) * 2);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                        outStream.writeBytes(msgToSend);


                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));

                        String ack = in.readLine();
                        if(ack.startsWith("DeleteDone")){

                            String[] inf = ack.split(":");
                            sendPort = inf[1].trim();

                            Log.d(TAG,"DeletedAtPort:"+sendPort);

                            socket.close();
                        }


                    }

                }catch (Exception e){
                    Log.d(TAG,"DeleteExc"+e);
                }


            }

            //Object result = asyncTask.exerte().get();


            return null;
        }





    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        Log.d(TAG,"Entered Delete: "+selection);

        if(selection.equals("*") || selection.equals("@")) {
            File[] files = getContext().getFilesDir().listFiles();
            for (File file : files) {

                file.delete();

            }

            if(selection.equals("*") && !predNode.isEmpty()){
                String deleteMsg = "Delete:"+succNode;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteMsg , cNode);
            }
        }
        else {

            File dir = getContext().getFilesDir();
            File file = new File(dir,selection);
            if(file.exists()) {
                file.delete();
                Log.d(TAG,"Deleting "+selection+" at: "+cNode);
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DelKey:" + selection, cNode);

                Log.d(TAG,"Couldnt delete: "+selection+ "passing to "+ succNode);
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
        // TODO Auto-generated method stub


        String key = values.getAsString("key");
        String value = values.getAsString("value");

        FileOutputStream outputStream;

        String keyHash="";
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.d(TAG,"EnteredInsert:"+key+":"+value);
        Log.d(TAG,"HashedValues:"+key+"KeyHash:"+keyHash+":Node:"+cNodeId+":Prev:"+predNodeId);
        if( (predNodeId.isEmpty() && succNodeId.isEmpty()) || (predNodeId.compareTo(keyHash)<0 && keyHash.compareTo(cNodeId)<=0 )
                || (predNodeId.compareTo(keyHash)<0 && predNodeId.compareTo(cNodeId)>0)
                || (predNodeId.compareTo(keyHash)>0 && cNodeId.compareTo(keyHash)>0 && predNodeId.compareTo(cNodeId)>0)) {
            try {
                //outputStream = openFileOutput(key, Context.MODE_PRIVATE);
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("GroupMessenger", "File write failed");
            }

            Log.d(TAG, "Inserted: "+values.toString());
        }
        else{
            String insertmsg= "Route:"+key+":"+value;

            Log.d(TAG,"Routing:"+succNode+":"+insertmsg);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertmsg, cNode);

        }

        return uri;
        //return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        String columnNames[] = {"key","value"};
        MatrixCursor cursor = new MatrixCursor(columnNames,1);

        String backup = selection;
        String[] info =new String[5];
        if(selection.startsWith("Searching")){
            info = selection.split(":");
            selection = info[1];
            String senderNode = info[2];
        }

        String keyHash="";
        try {
            keyHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        Log.d(TAG,"EnteredQuery:"+backup);
        if(selection!=null) {



            if(selection.equals("*") || selection.equals("@")) {
                File[] files = getContext().getFilesDir().listFiles();
                for (File file : files) {

                    String filename = file.toString();
                    String pattern = Pattern.quote(System.getProperty("file.separator"));
                    String[] splittedFileName =  filename.split(pattern);

                    String s = file.toString();
                    cursor = fetchValue(cursor,splittedFileName[splittedFileName.length-1]);
                    Log.d(TAG, "Logging files: " + splittedFileName[splittedFileName.length-1]);
                }

                if(selection.equals("*") && !predNode.isEmpty()){
                    String found = "FoundStar"+":*:*:"+succNode;


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, found, cNode);


                    Log.d(TAG, "BlockingUntilStar:" + selection);

                    try {
                        String starData = "";
                        starData = bq.take();

                        if(!starData.isEmpty()) {
                            String[] kvPairs = starData.split(":");

                            for (String kv : kvPairs) {
                                Log.d(TAG, "KVPair:" + kv);
                                String[] kvOut = kv.split("-");

                                cursor.addRow(new Object[]{kvOut[0], kvOut[1]});

                            }
                        }



                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }

            else if( (predNodeId.isEmpty() && succNodeId.isEmpty()) || (predNodeId.compareTo(keyHash)<0 && keyHash.compareTo(cNodeId)<=0 )
                    || (predNodeId.compareTo(keyHash)<0 && predNodeId.compareTo(cNodeId)>0)
                    || (predNodeId.compareTo(keyHash)>0 && cNodeId.compareTo(keyHash)>0 && predNodeId.compareTo(cNodeId)>0)) {

                Log.d(TAG,"CorrectCV Found:"+selection);

                String value = "";
                try {
                    StringBuilder builder = new StringBuilder();
                    FileInputStream inputStream = getContext().openFileInput(selection);
                    int ch;
                    while ((ch = inputStream.read()) != -1) {
                        builder.append((char) ch);
                    }
                    value = builder.toString();
                    Log.d(TAG,"RetrievedValueGotit:"+selection+":"+value);
                    inputStream.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "Unable to read file");
                } catch (IOException e) {
                    Log.e(TAG,"IO Exception while reading file");
                    e.printStackTrace();
                }catch(Exception e){
                    Log.e(TAG,"ExceptionFile:"+e);
                }



                Log.d(TAG,"RetrievedValue:"+selection+":"+value);
                Log.d(TAG,backup);
                if(!backup.startsWith("Searching"))
                    cursor.addRow(new Object[]{selection, value});
                else{

                    String foundKey = "FoundKey:"+selection+":"+value+":"+info[2];
                    Log.d(TAG,foundKey);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, foundKey, cNode);
                }

            }
            else if(!backup.startsWith("Searching")) {


                String querymsg = "Query:" + selection + ":" + cNode;

                Log.d(TAG, "BlockingUntilKey:" + selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, querymsg, cNode);


                try {
                    String xx = "";
                    xx = bq.take();

                    String[] f = xx.split(":");
                    String k = f[0];
                    String v = f[1];
                    Log.d(TAG, "DoneReturning:" + k + ":" + v);
                    cursor.addRow(new Object[]{k, v});

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                Log.d(TAG,"Whyyyyyyyyyy:"+selection);
                String querymsg = "";

                querymsg = "Query:" + info[1]+":" +info[2];

                Log.d(TAG,"ExecutingQuery:"+querymsg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, querymsg, cNode);
            }
            }

            Log.d(TAG,"Query: " +selection);

        return cursor;

        //return null;
    }

    public MatrixCursor fetchValue(MatrixCursor cursor,String selection){


        String value="";
        try {
            StringBuilder builder = new StringBuilder();
            FileInputStream inputStream = getContext().openFileInput(selection);
            int ch;
            while ((ch = inputStream.read()) != -1) {
                builder.append((char) ch);
            }
            value = builder.toString();
            inputStream.close();
        } catch (FileNotFoundException e) {
            Log.e("GroupMessenger", "Unable to read file");
        } catch (IOException e) {
            e.printStackTrace();
        }
        cursor.addRow(new Object[]{selection, value});
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }



    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class Node{
        private Node succ=null;
        private Node pred=null;
        private String port;
        private String node_id;

        public Node(String port,String node_id){
            this.port = port;
            this.node_id = node_id;
        }

        public void setPred(Node pred){
            this.pred = pred;

        }

        public void setSucc(Node succ){
            this.succ = succ;
        }

        public Node getPred(){
            return this.pred;
        }

        public Node getSucc(){
            return this.succ;
        }

        public String getNodeId(){
            return this.node_id;
        }

        public String getPortStr(){
            return this.port;
        }

    }
}
