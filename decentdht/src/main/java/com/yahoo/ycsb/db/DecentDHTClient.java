package com.yahoo.ycsb.db;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.json.JSONObject;

import com.decent.dht.DhtClientBinding;
import com.decent.dht.DhtClientBindingException;

/**
 * YCSB binding for DecentDHT.
 */
public class DecentDHTClient extends DB {

  /**
   * Private memebers such as DHT connections
   * or other handlers which can be used to contact DHT when reading and writting data.
   */

  private boolean isInited = false;
  private DhtClientBinding clientBind;

  public DecentDHTClient() {
    this.clientBind = new DhtClientBinding();
  }

  public void init() throws DBException {
    
    try {
      this.clientBind.init();
    } catch (DhtClientBindingException e) {
      throw new DBException(e.getMessage());
    }
    isInited = true;
  }

  public void cleanup() throws DBException {
    
    if (isInited) {
      try {
        this.clientBind.cleanup();
      } catch (DhtClientBindingException e) {
        throw new DBException(e.getMessage());
      }
      isInited = false;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    
    // byte[] buffer;
    String buffer;

    // table is ignored in this function.
    // Given the key, retrieve the value from DHT
    // Convert the data retrieved to Json doc.

    try {
      buffer = this.clientBind.read(key);
    } catch (DhtClientBindingException e) {
      return new Status("ERROR ", e.getMessage());
    }

    // The rest of the code should be similar in our implementation. 
    // I think we can just use the codes here without changes. 
    JSONObject json = new JSONObject(buffer);
    Set<String> fieldsToReturn = (fields == null ? json.keySet() : fields);

    for (String name : fieldsToReturn) {
      result.put(name, new StringByteIterator(json.getString(name)));
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }


  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values)
  {
    
    // table is ignored again.
    // No need to change the codes below.
    JSONObject json = new JSONObject();
    for (final Entry<String, ByteIterator> e : values.entrySet()) {
      json.put(e.getKey(), e.getValue().toString());
    }

    try {
      // Write the key and the value to our DHT here. Example: ioctx.write(key, json.toString());
      this.clientBind.insert(key, json.toString());
    } catch (DhtClientBindingException e) {
      return new Status("ERROR ", e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    
    try {
      
      // very simple function, just remove the data associated with the key given. table is not used.
      // Example: ioctx.remove(key);
      this.clientBind.delete(key);
    } catch (DhtClientBindingException e) {
      return new Status("ERROR ", e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    
    // Just call delete() function first and then call insert() function.
    // No need to change the code.
    Status rtn = delete(table, key);
    if (rtn.equals(Status.OK)) {
      return insert(table, key, values);
    }
    return rtn;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}