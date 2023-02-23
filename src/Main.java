
import DBDefinition.Catalog;
import IO.BufferManager;
import IO.StorageManager;
import SQLParsing.ParseBasicSQL;

import java.io.*;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        String dbLoc;
        int pageSize = 0;
        int bufferSize = 0;

        try {
            dbLoc = args[0];

            // Converts pageSize and bufferSize are Integer
            try {
                pageSize = Integer.parseInt(args[1]);
                bufferSize = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.out.println("System Error:");
                System.out.println("Usage: java Main <db loc> <page size> <buffer size>");
                System.exit(1);
            }

            // First check for if dbLoc is a valid path
            try {
                Paths.get(dbLoc);
            } catch (InvalidPathException | NullPointerException ex) {
                System.out.println("System Error:");
                System.out.println("Absolute path to database does not exist");
                System.exit(1);
            }

            // Second check for if dbLoc is a valid path
            File dbLocFolder = new File(dbLoc);
            if (!dbLocFolder.exists()) {
                System.out.println("System Error:");
                System.out.println("Absolute path to database does not exist");
                System.exit(1);
            }

            System.out.println("Welcome to NumberNineQL");
            System.out.println("Looking at " + dbLoc + " for existing db....");

            // Checking if a database exist in folder
            Catalog catalog;
            File catalogFile = new File(dbLoc + "/catalog.bin");
            if (catalogFile.exists()){
                System.out.println("Database found...");
                catalog = new Catalog(true, catalogFile, dbLoc, pageSize, bufferSize);
                System.out.println("Page Size: " + catalog.getPageSize() +
                        "\nBuffer Size: " + catalog.getBufferSize() );
            }
            else {
                System.out.println("No existing db found");
                System.out.println("Creating new db at " + dbLoc);
                catalog = new Catalog(false, catalogFile, dbLoc, pageSize, bufferSize);
                initializeDatabase(catalogFile, pageSize, bufferSize);
            }
            BufferManager bufferManager = new BufferManager(bufferSize);
            StorageManager storageManager = new StorageManager(catalog.getDbLocLoc());

            // Reads user input and calls necessary functions.
            //BufferedReader bfn = new BufferedReader(new InputStreamReader(System.in));
            Scanner sc = new Scanner(System.in);
            System.out.println("\nPlease enter commands, enter <quit> to shutdown the db\n");
            String userInput = "";
            try{
                ParseBasicSQL parser = new ParseBasicSQL();
                while(!userInput.equals("<quit>")) {

                    int count = 1;
                    do {
                        String tempStr = sc.nextLine().trim();
                        if(count == 1){
                            userInput = tempStr;
                        }
                        else {
                            userInput = userInput + " " + tempStr;
                        }

                        if(userInput.equals("<quit>")) {
                            break;
                        }
                        count++;
                    } while (!userInput.endsWith(";"));


                    userInput = userInput.replaceAll("\\(", " ( ");
                    userInput = userInput.replaceAll("\\)", " ) ");
                    userInput = userInput.replaceAll(",", " , ");
                    userInput = userInput.replaceAll(";", " ; ");
                    userInput = userInput.replaceAll("\\s+", " ");
                    String[] parsedInput = userInput.split(" ");
                    /*for(String a : parsedInput){
                        System.out.println(a);
                    }*/
                    parser.readInput(parsedInput, catalog, bufferManager, storageManager);

                    // New line for command
                    System.out.println();
                }

            } catch (Exception e) {
                System.out.println("System Error: input error");
            }

        }
        catch (IndexOutOfBoundsException e) {
            System.out.println("System Error:");
            System.out.println("Usage: java Main <db loc> <page size> <buffer size>");
            System.exit(1);
        }
    }

    public static void initializeDatabase(File catalogFile, int pageSize, int bufferSize)
        {
            try {
                // Create catalog.txt
                RandomAccessFile catalogWriter = new RandomAccessFile(catalogFile, "rw");
                catalogWriter.close();
                System.out.println("New db created successfully" +
                                    "\nPage Size: " + pageSize +
                                    "\nBuffer Size: " + bufferSize );

            } catch (IOException e) {
               System.out.println("System Error: File creation failed.");
            }
        }
}
