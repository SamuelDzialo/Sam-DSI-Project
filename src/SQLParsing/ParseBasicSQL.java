package SQLParsing;

import DBDefinition.*;
import DBDefinition.Record;
import IO.*;

import java.util.ArrayList;

public class ParseBasicSQL {
    public ParseBasicSQL() {

    }

    public void readInput(String[] parsedInput, Catalog catalog, BufferManager bufferManager,
                          StorageManager storageManager) {
        try{
            switch (parsedInput[0]) {
                case "create":

                    if(parsedInput[1].equals("table")) {
                        if(parsedInput[3].equals("(")) {
                            String tableName = parsedInput[2];
                            if(catalog.getTableNames().contains(tableName)) {
                                System.out.println("Table of name " + tableName + " already exists");
                                Error();
                                return;
                            } else {
                                parseCreateTable(parsedInput, storageManager, catalog, tableName, bufferManager);
                            }
                        }
                        else {
                            Error();
                            return;
                        }
                    } else {
                        Error();
                        return;
                    }
                    break;
                case "select":

                    if(parsedInput.length == 4) {
                        if(parsedInput[1].equals("*")) {
                            if(parsedInput[2].equals("from")){
                                if(parsedInput[3].endsWith(";")) {
                                    String tableName = parsedInput[3].substring(0,parsedInput[3].length()-1);
                                    if(catalog.getTableNames().contains(tableName)) {
                                        storageManager.select(tableName);
                                    } else {
                                        System.out.println("No such table " + tableName);
                                        Error();
                                    }
                                } else {
                                    Error();
                                    return;
                                }
                            } else {
                                Error();
                                return;
                            }
                        } else {
                            Error();
                            return;
                        }
                    }
                    break;
                case "insert":

                    if(parsedInput[1].equals("into")) {
                        if(catalog.getTableNames().contains(parsedInput[2])) {
                            String tableName = parsedInput[2];
                            if(parsedInput[3].equals("values")) {
                                if(parsedInput[4].equals("(")) {
                                    parseInsertValue(parsedInput, storageManager, catalog, tableName, bufferManager);
                                } else {
                                    Error();
                                    return;
                                }
                            } else {
                                Error();
                                return;
                            }
                        } else {
                            System.out.println("No such table " + parsedInput[2]);
                            Error();
                            return;
                        }
                    } else {
                        Error();
                        return;
                    }
                    break;
                case "display":
                    switch (parsedInput[1]) {
                        case "schema":
                            if(parsedInput[2].equals(";")){
                                catalog.displaySchema();
                            } else {
                                Error();
                                return;
                            }

                            break;
                        case "info":

                            if(parsedInput[3].equals(";")) {
                                String currTableName = parsedInput[2];
                                if(catalog.getTableNames().contains(currTableName)) {
                                    catalog.displayInfo(currTableName);
                                } else {
                                    System.out.println("No such table " + currTableName);
                                    Error();
                                    return;
                                }
                            } else {
                                Error();
                                return;
                            }
                            break;
                        default:
                            invalidCommand();
                            break;
                    }
                    break;
                case "<quit>":

                    System.out.println("Safely shutting down the database...");
                    catalog.writeToDisk();
                    break;
                default:
                    invalidCommand();
                    break;
            }
        } catch( Exception e) {
            invalidCommand();
        }
    }

    public void parseInsertValue(String[] parsedInput, StorageManager storageManager, Catalog catalog,
                                 String tableName, BufferManager bufferManager) {
        try {
            TableSchema currTableSchema = catalog.getTableSchemaByTableName(tableName);
            ArrayList<AttributeSchema> attrList = currTableSchema.getAttrList();
            ArrayList<Record> recordsList = new ArrayList<>();
            int i = 5;


            while(!parsedInput[i].equals(";")) {
                int attrListCount = 0;
                ArrayList<Object> values = new ArrayList<>();
                for(AttributeSchema attributeSchema : attrList) {
                    attrListCount++;
                    switch(attributeSchema.getType()) {
                        case INTEGER -> {
                            try {
                                int intValue = Integer.parseInt(parsedInput[i]);
                                values.add(intValue);
                            } catch (Exception e) {
                                System.out.println("Invalid data type: expected (integer)");
                                Error();
                                return;
                            }
                        }
                        case DOUBLE -> {
                            try {
                                double doubleValue = Double.parseDouble(parsedInput[i]);
                                values.add(doubleValue);
                            } catch (Exception e) {
                                System.out.println("Invalid data type: expected (double)");
                                Error();
                                return;
                            }

                        }
                        case BOOLEAN -> {
                            try {
                                boolean boolValue = Boolean.parseBoolean(parsedInput[i]);
                                values.add(boolValue);
                            } catch (Exception e) {
                                System.out.println("Invalid data type: expected (boolean)");
                                Error();
                                return;
                            }

                        }
                        case CHAR -> {
                            try {
                                if(parsedInput[i].length() <= attributeSchema.getSize()) {
                                    String charValue = parsedInput[i];
                                    values.add(charValue);
                                } else {
                                    System.out.println("char(" + attributeSchema.getSize() +
                                            ") can only accept " + attributeSchema + "chars; " +
                                            parsedInput[i] + " is " + parsedInput[i].length());
                                    Error();
                                    return;
                                }

                            } catch (Exception e) {
                                System.out.println("Invalid data type: expected (char)");
                                Error();
                                return;
                            }
                        }
                        case VARCHAR -> {
                            try {
                                if(parsedInput[i].length() <= attributeSchema.getSize()) {
                                    String varCharValue = parsedInput[i];
                                    values.add(varCharValue);
                                } else {
                                    System.out.println("varchar(" + attributeSchema.getSize() +
                                            ") can only accept " + attributeSchema + "chars; " +
                                            parsedInput[i] + " is " + parsedInput[i].length());
                                    Error();
                                    return;
                                }

                            } catch (Exception e) {
                                System.out.println("Invalid data type: expected (char)");
                                Error();
                                return;
                            }
                        }
                    }
                    i++;
                    if(attrListCount == attrList.size()){
                        if(parsedInput[i].equals(")")) {
                            i++;
                            if (parsedInput[i].equals(",")) {
                                i++;
                                if (parsedInput[i].equals("(")) {
                                    i++;
                                } else {
                                    System.out.println("Expected (");
                                    Error();
                                    return;
                                }
                            } else if (parsedInput[i].equals(";")) {
                                break;
                            } else {
                                System.out.println("Expected ,");
                                Error();
                                return;
                            }
                        } else {
                            System.out.println("Expected )");
                            Error();
                            return;
                        }
                    }
                }
                Record record = new Record(values);
                recordsList.add(record);
            }
            for(Record record: recordsList) {
                storageManager.insertRecord(catalog, tableName, record, bufferManager);
            }
        } catch (Exception e) {
            System.out.println("bad value.");
            Error();
        }

    }

    public void parseCreateTable(String[] parsedInput, StorageManager storageManager, Catalog catalog,
                                 String tableName, BufferManager bufferManager) {
        int i = 4;
        String attrName;
        String attrType;
        int size = 0;
        boolean end = false;
        int PKCount = 0;
        ArrayList<AttributeSchema> attrList = new ArrayList<>();
        do {
            int isPK = 0;
            // Assign the attribute name
            attrName = parsedInput[i];

            // Assign the attribute type
            i++;
            attrType = parsedInput[i];

            // if Char/varChar type - get size
            i++;
            if(attrType.equals("char") || attrType.equals("varchar")) {
                if (parsedInput[i].equals("(")) {
                    i++;
                    String charSize = parsedInput[i];
                    size = Integer.parseInt(charSize);
                    i++;
                    if (parsedInput[i].equals(")")) {
                        i++;
                    } else {
                        System.out.println("Expected )");
                        Error();
                        return;
                    }
                } else {
                    System.out.println("Expected (");
                    Error();
                    return;
                }
            }

            if(parsedInput[i].equals(")")){
                i++;
                if(parsedInput[i].equals(";")) {
                    end = true;
                }
            }

            if(!end){
                if(parsedInput[i].equals("primarykey")){
                    i++;
                    isPK = 1;
                    if(parsedInput[i].equals(")") && parsedInput[i+1].equals(";")){
                        end = true;
                    }
                    PKCount++;

                    if(PKCount > 1) {
                        System.out.println("More then one primarykey");
                        System.out.println("ERROR");
                        return;
                    }
                }
                if (parsedInput[i].equals(",")){
                    i++;
                }
            }
            attrList.add(new AttributeSchema(attrName, attrType, isPK, size));

            if(parsedInput.length-1 == i) {
                end = true;
            }
        } while (!end);

        if(PKCount == 0){
            System.out.println("No primary key defined");
            Error();
            return;
        }
        storageManager.createTable(catalog, tableName, attrList);
    }

    public void Error() {
        System.out.println("ERROR");
    }

    public void invalidCommand() {
        System.out.println("\nInvalid command. \nERROR");
    }
}
