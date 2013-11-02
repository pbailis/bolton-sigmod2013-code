/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import edu.berkeley.lipstick.config.Config;
import edu.berkeley.lipstick.copsclient.ClientConfig;
import edu.berkeley.lipstick.copsclient.CopsStats;
import edu.berkeley.lipstick.shim.LipstickShimExplicitCausality;
import edu.berkeley.lipstick.util.DataWrapper;


//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 1.0.6 client for YCSB framework
 */
public class BoltOnCassandraClient10 extends DB
{
  static Random random = new Random();
  public static final int Ok = 0;
  public static final int Error = -1;
  public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

  public int ConnectionRetries;
  public int OperationRetries;
  public String column_family;

  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String LENGTH_DISTRIBUTION_PROPERTY = "bolton.explicitdistribution";
  public static final String LENGTH_DISTRIBUTION_PROPERTY_EXPONENTIAL = "exponential";
  public static final String LENGTH_DISTRIBUTION_PROPERTY_CONSTANT = "constant";
  public static final String LENGTH_DISTRIBUTION_PROPERTY_HISTOGRAM = "histogram";
  public static final String LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = LENGTH_DISTRIBUTION_PROPERTY_EXPONENTIAL;

  public static final String EXPONENTIAL_DISTRIBUTION_GAMMA_PROPERTY = "bolton.explicitdistribution.gamma";
  public static final String EXPONENTIAL_DISTRIBUTION_GAMMA_PROPERTY_DEFAULT = ".1";

  public static final String CONSTANT_DISTRIBUTION_LENGTH_PROPERTY = "bolton.constantdistribution.length";
  public static final String CONSTANT_DISTRIBUTION_LENGTH_PROPERTY_DEFAULT = "10";

  public static final String HISTOGRAM_DISTRIBUTION_FILE_PROPERTY = "bolton.histogramdistribution.file";
  public static final String HISTOGRAM_DISTRIBUTION_FILE_PROPERTY_DEFAULT = "twitter-hist.txt";

  boolean _debug = false;

  IntegerGenerator chainLengthGenerator;
  int desiredChainLength = 0;
  int currentChainLength = 0;
  DataWrapper lastWrite;

  String _table = "";
  Exception errorexception = null;


  static LipstickShimExplicitCausality shim = null;
  static long clientsIn = 0;
  static Lock shimLock = new ReentrantLock();


  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
      OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
      OPERATION_RETRY_PROPERTY_DEFAULT));

      try {

            shimLock.lock();
            if(shim == null) {
              shim = new LipstickShimExplicitCausality();
              shim.open();
            }
            clientsIn++;

            shimLock.unlock();

            String distributionType = getProperties().getProperty(LENGTH_DISTRIBUTION_PROPERTY,
                                                                  LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

            if(distributionType.equals(LENGTH_DISTRIBUTION_PROPERTY_EXPONENTIAL))
                chainLengthGenerator = new ExponentialGenerator(
                        Double.parseDouble(getProperties()
                        .getProperty(EXPONENTIAL_DISTRIBUTION_GAMMA_PROPERTY,
                        EXPONENTIAL_DISTRIBUTION_GAMMA_PROPERTY_DEFAULT)));
            else if(distributionType.equals(LENGTH_DISTRIBUTION_PROPERTY_CONSTANT))
                chainLengthGenerator = new ConstantIntegerGenerator(Integer.parseInt(
                        getProperties().getProperty(CONSTANT_DISTRIBUTION_LENGTH_PROPERTY,
                                                    CONSTANT_DISTRIBUTION_LENGTH_PROPERTY_DEFAULT)));
            else if(distributionType.equals(LENGTH_DISTRIBUTION_PROPERTY_HISTOGRAM))
                chainLengthGenerator = new HistogramGenerator(
                        getProperties().getProperty(HISTOGRAM_DISTRIBUTION_FILE_PROPERTY,
                                                    HISTOGRAM_DISTRIBUTION_FILE_PROPERTY_DEFAULT));

            //TOOD: reply many

        }
        catch (Exception te) {
            te.printStackTrace();
        }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
      try {
        shimLock.lock();
        clientsIn--;

        if(clientsIn == 0)
            shim.close();
        shimLock.unlock();
      }
      catch(Exception e) {
          e.printStackTrace();
      }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
  {
    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
          DataWrapper shimRet = shim.get(key);
          ByteArrayByteIterator realRet = null;
          if(shimRet != null)
              realRet = new ByteArrayByteIterator((byte []) shimRet.getValue());
          result.put(key, realRet);

        return Ok;
      } catch (Exception e)
      {
          e.printStackTrace();
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }


    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result)
  {
    return 0;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, ByteIterator> values)
  {
    return insert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */

  public int insert(String table, String key, HashMap<String, ByteIterator> values)
  {
    if(currentChainLength >= desiredChainLength) {
        desiredChainLength = chainLengthGenerator.nextInt();
        currentChainLength = 0;
        lastWrite = null;
    }

    currentChainLength++;

    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
          DataWrapper ret =  shim.put_after(key, values.get(values.keySet().iterator().next()).toArray(), lastWrite);

          lastWrite = ret;
        return Ok;
      } catch (Exception e)
      {
          System.out.print(e.getMessage());
          e.printStackTrace();
          errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }

    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key)
  {
    return 0;
  }

  public static void main(String[] args)
  {
    BoltOnCassandraClient10 cli = new BoltOnCassandraClient10();

    Properties props = new Properties();

    props.setProperty("hosts", args[0]);
    cli.setProperties(props);

    try
    {
      cli.init();
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }

    HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
    vals.put("age", new StringByteIterator("57"));
    vals.put("middlename", new StringByteIterator("bradley"));
    vals.put("favoritecolor", new StringByteIterator("blue"));
    int res = cli.insert("usertable", "BrianFrankCooper", vals);
    res = cli.insert("usertable", "abc", vals);
    res = cli.insert("usertable", "bar", vals);

    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    HashSet<String> fields = new HashSet<String>();
    fields.add("middlename");
    fields.add("age");
    fields.add("favoritecolor");
    res = cli.read("usertable", "abc", null, result);
    System.out.println("Result of read: " + res);
    for (String s : result.keySet())
    {
      System.out.println("[" + s + "]=[" + result.get(s) + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
   }
}