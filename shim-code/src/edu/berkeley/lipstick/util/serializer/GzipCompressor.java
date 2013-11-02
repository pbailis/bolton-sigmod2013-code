package edu.berkeley.lipstick.util.serializer;

import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressor {
    public static byte[] compress(byte[] input) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream out = new GZIPOutputStream(bos);
        out.write(input, 0, input.length);
        out.close();
        return bos.toByteArray();
    }

    public static byte[] decompress(byte[] input) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(input)), out);
        return out.toByteArray();
    }
}