package IO;

import java.util.ArrayList;

public class Buffer {
    //TODO set this to user input
    private static final int MAXRECORDS = 5;
    private ArrayList<Page> pages;
    public Buffer(){
        this.pages = new ArrayList<>();
    }

    public Page readPage(Page p){
        for(int i=0; i<pages.size(); i++){
            if(pages.get(i).equals(p)){
                Page temp = pages.remove(i);
                pages.add(temp);
                return temp;
            }
        }
        return null;
    }

    public void insertPage(Page p){
        if(pages.size() >= MAXRECORDS){
            //TODO Write out pages 0
            pages.remove(0);
        }
        pages.add(p);
    }

}
