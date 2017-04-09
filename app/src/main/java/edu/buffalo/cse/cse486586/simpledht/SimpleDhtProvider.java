package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
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

    ArrayList<Node> nodeList = new ArrayList<Node>();
    String predNode = "";
    String succNode = "";

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

        if(myPort.equals(REMOTE_PORT0)){
            String node_id = "";
            try {
                node_id = genHash(portStr);
            } catch (NoSuchAlgorithmException e) {
                Log.d(TAG,e+"");
            }
            Node n = new Node(portStr,node_id);
            nodeList.add(n);
            Log.d(TAG,"NodeList start check:"+nodeList.size());

        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
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

    private class ServerTask extends AsyncTask<ServerSocket, String, String> {

        @Override
        protected String doInBackground(ServerSocket... sockets) {
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
                            Log.d(TAG,"Second Node Joining");
                            currNode.setSucc(newNode);
                            currNode.setPred(newNode);
                            newNode.setSucc(currNode);
                            newNode.setPred(currNode);
                        }
                        else if(currNode.getNodeId().compareTo(newNode.getNodeId())<0 ){
                            Log.d(TAG,"GreaterThan");
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
                            Log.d(TAG,"LesserThan");
                            Node pred= currNode.getPred();
                            while(pred.getNodeId().compareTo(newNode.getNodeId())<0){
                                pred = pred.getPred();
                                if(pred.getNodeId().compareTo(pred.getSucc().getNodeId())<0)
                                    break;
                            }
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

                        outStream.writeBytes("Done\n");
                    }




                }

            } catch (Exception e) {
                Log.e(TAG, e + "");
            }

            return "";
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0]+"\n";




            String remotePort;
            String nodeInfo="";
            try {

                for (int i = 0; i < portList.size(); i++) {

                    remotePort = portList.get(i);
                    if(msgToSend.startsWith("NodeJoin") && remotePort!=REMOTE_PORT0)
                        continue;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

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



            Log.d(TAG,nodeInfo);
            String[] connInfo = nodeInfo.split(":::");
            Log.d(TAG,"ConnSize:"+connInfo.length);

            try {


                    for (String conn : connInfo) {

                        Log.d(TAG,conn);


                        String[] portInfo = conn.split("-");
                        remotePort = Integer.toString(Integer.parseInt(portInfo[0])*2);


                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));


                        String sending="Info-"+portInfo[1]+"-"+portInfo[2]+"\n";
                        outStream.writeBytes(sending);

                        outStream.flush();

                        Log.d(TAG, "SentNodeInfo:" +portInfo[0]+"-Pred:"+portInfo[1]+"-Succ:"+portInfo[2]);
                        String ack = in.readLine();


                        Log.e(TAG, ack + remotePort+" - checking ACK");
                        if (ack.startsWith("Done")) {

                            socket.close();
                        }

                    }
                }
            catch (Exception e){
                    Log.d(TAG,e+"");
                }


            return null;
        }



    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

        try {
            //outputStream = openFileOutput(key, Context.MODE_PRIVATE);
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("GroupMessenger" ,"File write failed");
        }


        Log.d(TAG, "Inserted: "+values.toString());
        return uri;
        //return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        String columnNames[] = {"key","value"};
        MatrixCursor cursor = new MatrixCursor(columnNames,1);

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

            }
            else {
                cursor = fetchValue(cursor, selection);
            }
            Log.d(TAG,"Query: " +selection);
        }
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
