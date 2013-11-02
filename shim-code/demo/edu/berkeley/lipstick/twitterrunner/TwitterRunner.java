package edu.berkeley.lipstick.twitterrunner;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.shim.LipstickShimExplicitCausality;
import edu.berkeley.lipstick.util.DWProto;
import edu.berkeley.lipstick.util.DataWrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class TwitterRunner {

    public static void main(String[] args) throws Exception {
        LipstickShimExplicitCausality shim = new LipstickShimExplicitCausality();

        shim.open();
        DataWrapper hist = null;

        BufferedReader br = new BufferedReader(new FileReader("tweet/conversations.out"));
        String line = null;

        boolean newConvo = true;

        BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/conversations.length"));

        int convolen = -1;

        while ((line = br.readLine()) != null)
        {
            line = line.replace("&gt;", "").replace("&lt", "");

            if(line.compareTo("") == 0 || line.compareTo("\n") == 0) {
                if(convolen != -1)
                    bw.write(String.format("%d\n", convolen));
                convolen = 0;
                newConvo = true;
                continue;
            }

            convolen++;

            String[] keysplit = line.split("\t");

            if(keysplit.length == 0) {
                continue;
            }

            String key = keysplit[0];

            /*
            DWProto.TwitterMsg msg = DWProto.TwitterMsg.newBuilder().setMessage(line).build();

            if(msg.getSerializedSize() > 300) {
                System.out.println();
                System.out.println(msg.getSerializedSize());
                System.out.println(line);
            }

            if(newConvo) {
                hist = shim.put_at_start(key, msg.toByteArray());
                newConvo = false;
            }
            else {
                hist = shim.put_after(key,  msg.toByteArray(), hist);
            }
            */
        }


        System.out.println("Written: "+ Config.getBytesWritten());
        shim.close();
    }
}