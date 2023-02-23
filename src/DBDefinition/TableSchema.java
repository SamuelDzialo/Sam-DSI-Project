package DBDefinition;

import java.io.RandomAccessFile;
import java.util.ArrayList;

public class TableSchema {
    private String tableName;
    private ArrayList<AttributeSchema> attrList;
    private ArrayList<Integer> pageOrder;
    private int recordCount;

    public TableSchema(String tableName, ArrayList<AttributeSchema> attrList){
        this.tableName = tableName;
        this.attrList = attrList;
        this.pageOrder = new ArrayList<>();
        this.recordCount = 0;
    }

    public TableSchema(){
        this.tableName = "";
        this.attrList = new ArrayList<>();
        this.pageOrder = new ArrayList<>();
        this.recordCount = 0;
    }

    public ArrayList<AttributeSchema> getAttrList() {
        return attrList;
    }

    public String getTableName(){ return tableName; }

    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }

    public void displaySchema() {
        System.out.println("Table name: " + tableName);
        System.out.println("Table Schema:\t");

        int i = 1;
        for (AttributeSchema attributeSchema : attrList) {
            attributeSchema.displaySchema();
            if(i < attrList.size()) {
                System.out.println();
            }
            i++;
        }

        System.out.println("\nPage: " + pageOrder.size());
        System.out.println("Records: " + recordCount);
    }

    public TableSchema readFromDisk(RandomAccessFile catalogReader) {
        try {
            int tableNameLength = catalogReader.readInt();
            char[] tableNameArr = new char[tableNameLength];
            for(int j = 0; j < tableNameArr.length; j++) {
                tableNameArr[j] = catalogReader.readChar();
            }
            this.tableName = new String(tableNameArr);

            int attrNum = catalogReader.readInt();
            for(int k = 0; k < attrNum; k++){
                AttributeSchema attributeSchema = new AttributeSchema();
                attributeSchema.readFromDisk(catalogReader);
                this.attrList.add(attributeSchema);
            }

            catalogReader.writeInt(pageOrder.size());
            catalogReader.writeInt(recordCount);

        } catch (Exception e) {
            System.out.println("Error reading schema from catalog");
        }
        return new TableSchema(tableName,attrList);
    }

    public void writeToDisk(RandomAccessFile catalogWriter){
        try {
            catalogWriter.writeInt(tableName.length());
            for(int i = 0; i < tableName.length(); i++) {
                catalogWriter.writeChar(tableName.charAt(i));
            }

            catalogWriter.writeInt(attrList.size());
            for (AttributeSchema attributeSchema : attrList) {
                attributeSchema.writeToDisk(catalogWriter);
            }
            catalogWriter.writeInt(pageOrder.size());
            catalogWriter.writeInt(recordCount);
        } catch (Exception e) {
            System.out.println("Error writing schema to catalog");
        }
    }
}
