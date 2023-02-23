package IO;

import DBDefinition.*;
import DBDefinition.Record;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class StorageManager {
    private final File databaseHome;

    public StorageManager(String databasePath){
        databaseHome = new File(databasePath);
    }

    public void createTable(Catalog catalog, String tableName, ArrayList<AttributeSchema> attrList) {

        catalog.addTableSchema(new TableSchema(tableName, attrList));

        String tableLoc = catalog.getDbLocLoc() + "/" + tableName + ".bin";
        try {
            new RandomAccessFile(tableLoc,"rw");
        } catch(Exception e) {
            System.out.println("Error creating table " + tableName + " at \n" + tableLoc);
        }

        System.out.println("SUCCESS");
    }

    /**
     * Inserts a new record into a table
     * @param tableName The name of the destination table
     * @param newRecord the record to insert
     * @return true on a successful insert, false otherwise
     */
    public boolean insertRecord(Catalog catalog, String tableName, Record newRecord, BufferManager bufferManager){
        newRecord.printRecord();
        TableSchema tableSchema = catalog.getTableSchemaByTableName(tableName);

        // If a page doesn't exist
        if(tableSchema.getPageOrder().size() == 0){
            // Create a new page
            Page page = new Page(tableName, newRecord, catalog.getTableSchemaByTableName(tableName));
            bufferManager.InsertPageInBuffer(page);
            return true;
        }
        //TODO
        System.out.println("INSERT RECORD - STORAGE_MANAGER.JAVA");
        return false;
    }

    /**
     * retrieves all rows of a table from memory
     * @param table The table to select from
     * @return A list of records retrieved
     */
    public ArrayList<Record> select(String table){
        //TODO
        System.out.println("SELECT (IN STORAGEMANAGER.JAVA) - NEEDS TO BE CODED.");
        return null;
    }


}
