/**
 * This class provides functions to process Shakespeare.
 * It uses Apache Spark to analyze large text files.
 *
 * @author - Ruta Deshpande
 * Email - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.Scanner;

public class ShakespeareAnalytics {
    static SparkConf conf = new SparkConf().setMaster("local").setAppName("ShakespeareAnalytics");
    static JavaSparkContext sparkContext = new JavaSparkContext(conf);;
    static JavaRDD<String> inputFile;

    /**
     * Initializes the Spark context and reads the input file.
     * @param fileName the name of the input file to be processed
     */
    public static void init(String fileName) {
        inputFile = sparkContext.textFile(fileName);
    }

    /**
     * Counts the number of lines in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countNumLines(String fileName) {
        init(fileName);
        long numLines = inputFile.count(); // counting the number of lines
        System.out.println("Number of lines in the file \"" + fileName + "\": " + numLines);
    }

    /**
     * Counts the number of words in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countNumWords(String fileName) {
        init(fileName);
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));//tokenize the text
        long numWords = wordsFromFile.count();
        System.out.println("Number of words in the file \"" + fileName + "\": " + numWords);
    }

    /**
     * Counts the number of distinct words in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countNumDistinctWords(String fileName) {
        init(fileName);
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));//tokenize the text
        long numWords = wordsFromFile.distinct().count();
        System.out.println("Number of distinct words in the file \"" + fileName + "\": " + numWords);
    }


    /**
     * Counts the number of symbols in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countNumSymbols(String fileName) {
        init(fileName);
        JavaRDD<String> symbolsFromFile = inputFile.flatMap(line -> Arrays.asList(line.split(""))).filter(symbol -> !symbol.isEmpty());
        long numSymbols = symbolsFromFile.count();
        System.out.println("Number of symbols in the file \"" + fileName + "\": " + numSymbols);
    }

    /**
     * Counts the number of distinct symbols in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countDistinctNumSymbols(String fileName) {
        init(fileName);
        JavaRDD<String> symbolsFromFile = inputFile.flatMap(line -> Arrays.asList(line.split(""))).filter(symbol -> !symbol.isEmpty()).distinct();
        long numDistinctSymbols = symbolsFromFile.count();
        System.out.println("Number of distinct symbols in the file \"" + fileName + "\": " + numDistinctSymbols);
    }

    /**
     * Counts the number of distinct letters in the input file.
     * @param fileName the name of the input file to be processed
     */
    private static void countNumLetters(String fileName) {
        init(fileName);
        JavaRDD<String> lettersFromFile = inputFile.flatMap(line -> Arrays.asList(line.split(""))).filter(c -> c.matches("[a-zA-Z]")) // filter out non-letter characters
                .distinct(); // get only the unique letters
        long numLetters = lettersFromFile.count();
        System.out.println("Number of distinct letters in the file \"" + fileName + "\": " + numLetters);
    }

    /**
     * Searches for a specific word in the input file and prints the lines that contain it.
     * @param fileName the name of the input file to be searched
     * @param word the word to be searched for
     */
    private static void searchWord(String fileName, String word) {
        init(fileName);
        JavaRDD<String> linesWithWord = inputFile.filter(line -> line.contains(word));
        System.out.println("Lines in the file \"" + fileName + "\" that contain the word \"" + word + "\":");
        linesWithWord.foreach(line -> System.out.println(line));
    }

    /**
     * Prompts the user to enter a word and returns it as input.
     * @return the word entered by the user
     */
    public static String input() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the word you want to search for: ");
        String searchWord = scanner.nextLine();
        return searchWord;
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        countNumLines(args[0]);
        countNumWords(args[0]);
        countNumDistinctWords(args[0]);
        countNumSymbols(args[0]);
        countDistinctNumSymbols(args[0]);
        countNumLetters(args[0]);
        searchWord(args[0], input());
    }
}