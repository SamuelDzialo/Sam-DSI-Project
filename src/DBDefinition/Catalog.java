package DBDefinition;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class Catalog {

    private String catalogLoc;
    private String dbLoc;
    private List<String> tableNames;
    private int pageSize;
    private int bufferSize;
    private int tableNum;
    private HashMap<String,TableSchema> tableHashMap;

    public Catalog(boolean exist, File catalogLoc, String dbLoc, int pageSize, int bufferSize){
        try {
            this.catalogLoc = catalogLoc.toString();
            this.dbLoc = dbLoc;
            this.bufferSize = bufferSize;
            this.tableNames = new ArrayList<>();
            //this.tableSchemas = new ArrayList<>();
            this.tableHashMap = new HashMap<>();

            if(!exist){
                this.pageSize = pageSize;
                this.tableNum = 0;
            }
            else {
                System.out.println("Ignoring provided pages size, using stored page size");
                readFromDisk();
            }
        } catch (Exception e) {
            System.out.println("Error initializing Catalog");
        }
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public String getCatalogLoc(){
        return catalogLoc;
    }

    public String getDbLocLoc(){
        return dbLoc;
    }

    public int getPageSize(){
        return pageSize;
    }

    public int getBufferSize(){
        return bufferSize;
    }

    public TableSchema getTableSchemaByTableName (String tableName) {
        return tableHashMap.get(tableName);
    }

    public void addTableSchema(TableSchema tableSchema) {
        this.tableHashMap.put(tableSchema.getTableName(),tableSchema);
        this.tableNames.add(tableSchema.getTableName());
        this.tableNum++;
    }

    public void displaySchema() {
        try {
            System.out.println("DB Location: " + dbLoc);
            System.out.println("Page Size: " + pageSize);
            System.out.println("Buffer Size: " + bufferSize);

            System.out.println();
            if(tableNames.size() == 0) {
                System.out.println("No tables to display");
            }else {
                System.out.println("Tables:\n");
                for(String name : tableHashMap.keySet()) {
                    tableHashMap.get(name).displaySchema();
                    System.out.println();
                }
            }
            System.out.println("SUCCESS");
        } catch (Exception e) {
            System.out.println("Error opening catalog");
        }
    }

    public void displayInfo(String tableName) {
        TableSchema tableSchema = tableHashMap.get(tableName);
        tableSchema.displaySchema();
        System.out.println("SUCCESS");
    }

    public void readFromDisk() {
        try {
            RandomAccessFile catalogReader = new RandomAccessFile(catalogLoc, "r");
            catalogReader.seek(0);
            this.pageSize = catalogReader.readInt();
            this.tableNum = catalogReader.readInt();

            for(int i = 0; i < tableNum; i++) {
                TableSchema tableSchema = new TableSchema();
                tableSchema.readFromDisk(catalogReader);
                tableHashMap.put(tableSchema.getTableName(),tableSchema);
                this.tableNames.add(tableSchema.getTableName());
            }
            catalogReader.close();
        } catch (Exception e) {
            System.out.println("Error reading from catalog");
        }
    }

    public void writeToDisk() {
        try{
            System.out.println("Saving catalog...");
            RandomAccessFile catalogWriter = new RandomAccessFile(catalogLoc, "rw");

            catalogWriter.seek(0);
            catalogWriter.writeInt(pageSize);
            catalogWriter.writeInt(tableNum);

            for(String name : tableHashMap.keySet()) {
                tableHashMap.get(name).writeToDisk(catalogWriter);
            }

            catalogWriter.close();
        } catch (Exception e) {
            System.out.println("Error saving catalog");
        }
    }
}
