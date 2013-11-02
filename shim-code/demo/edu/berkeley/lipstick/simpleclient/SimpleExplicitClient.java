package edu.berkeley.lipstick.simpleclient;

import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.shim.LipstickShimExplicitCausality;
import edu.berkeley.lipstick.util.DataWrapper;

public class SimpleExplicitClient {

    public static void main(String[] args) throws Exception {
        LipstickShimExplicitCausality shim = new LipstickShimExplicitCausality();

        shim.open();
        DataWrapper hist;
        hist = shim.put_at_start("Foobar", "Baz");
        hist = shim.put_after("Foobar", "Baaz", hist);
        shim.put_at_start("Lololol", "Baaaz");
        System.out.println(shim.get("Lololol").getValue());
        System.out.println("Written: "+ Config.getBytesWritten());
        System.out.println("Read: "+ Config.getBytesRead());
        shim.close();
    }
}