
// import necessary packages
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleThread {
    // to handle exceptions include throws
    public static void main(String[] args)
            throws IOException {
        long start = System.currentTimeMillis();
        HashMap<String, Integer> intermediate = new HashMap<>();
        HashMap<String, Integer> temp = new HashMap<>();

        // list that holds strings of a file
        List<String> places = new ArrayList<String>();

        // load data from file
        BufferedReader bf1 = new BufferedReader(
                new FileReader("/home/kehsihba/Documents/mgs655/SearchPlaces/src/places"));

        // read entire line as string
        String line1 = bf1.readLine();

        // checking for end of file
        while (line1 != null) {
            if (line1.trim().length() != 0) {
                places.add(line1.toLowerCase());
            }
            line1 = bf1.readLine();
        }

        // closing bufferreader object
        bf1.close();

        // Fetch all the input files
        File folder = new File("/home/kehsihba/Documents/mgs655/input");
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                temp = SingleThread.mapper("/home/kehsihba/Documents/mgs655/input/" + listOfFiles[i].getName(), places);

                for (Map.Entry<String, Integer> e : temp.entrySet()) {
                    if (intermediate.containsKey(e.getKey())) {
                        Integer count = intermediate.get(e.getKey());
                        intermediate.put(e.getKey(), count + e.getValue());
                    } else {
                        intermediate.put(e.getKey(), 1);

                    }
                }
            }
        }

        for (Map.Entry<String, Integer> e : intermediate.entrySet()) {
            System.out.println(e.getKey() + " " + e.getValue());
        }

        long end = System.currentTimeMillis();
        System.out.println("Elapsed Time in milli seconds: " + (end - start));

    }

    public static HashMap<String, Integer> mapper(String filepath, List<String> placesArray)
            throws IOException {

        HashMap<String, Integer> map = new HashMap<>();

        // list that holds strings of a file
        List<String> listOfWords = new ArrayList<String>();

        // load data from file
        BufferedReader bf = new BufferedReader(
                new FileReader(filepath));

        // read entire line as string
        String line = bf.readLine();

        // checking for end of file
        while (line != null) {
            if (line.trim().length() != 0) {
                line = line.toLowerCase();
                String[] lineWords = line.split(" ");
                listOfWords.addAll(Arrays.asList(lineWords));
            }
            line = bf.readLine();
        }

        // closing bufferreader object
        bf.close();

        String[] wordsArray = listOfWords.toArray(new String[0]);

        // Check if token exists in the "places" list
        String prevToken = "";
        for (int i = 0; i < wordsArray.length; i++) {
            String token = wordsArray[i].trim();
            if (token == " " || token == "\n")
                continue;

            if (prevToken != "") {
                prevToken = prevToken + " " + token;
                if (placesArray.contains(prevToken)) {
                    // System.out.println(prevToken);
                    if (map.containsKey(prevToken)) {
                        Integer count = map.get(prevToken);
                        map.put(prevToken, count + 1);
                    } else {
                        map.put(prevToken, 1);
                    }
                }
            }

            if (placesArray.contains(token)) {
                // System.out.println(token);
                if (map.containsKey(token)) {
                    Integer count = map.get(token);
                    map.put(token, count + 1);
                } else {
                    map.put(token, 1);
                }
            }

            prevToken = token;

        }

        return map;
    }

}