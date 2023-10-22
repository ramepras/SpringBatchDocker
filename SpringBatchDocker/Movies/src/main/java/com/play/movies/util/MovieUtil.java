package com.play.movies.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieUtil {
    public static Long extractMovieYear(String inputString) {
        String ret = "";
        // Regular expression to match a year in brackets
        String regex = "\\((\\d{4})\\)";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(inputString);

        if (matcher.find()) {
            ret =  matcher.group(1).trim();  // Extract the year
        } else {
            ret = "1900";
        }
        return Long.valueOf(ret);
    }
    public static String extractMovieTitle(String inputString) {
        String movieName = extractMovieName(inputString);
        String cleanedString = removeSpecialCharacters(movieName);
        return capitalizeFirstLetterAfterSpace(cleanedString);
    }
    public static String extractMovieName(String inputString) {
        String regex = "\\s*\\(\\d{4}\\)\\s*";
        return inputString.replaceAll(regex, "").trim();
    }
    public static String extractMovieGenres(String inputString) {
        return inputString.replaceAll("\\|", ",");
    }
    private static String removeSpecialCharacters(String inputString) {
        String regex = "[^a-zA-Z0-9\\s]";
        inputString = inputString.replaceAll(regex, "").trim();
        inputString = isNotNullNotEmpty(inputString) ? inputString : "No Title";
        return inputString.trim();
    }
    private static boolean isNotNullNotEmpty(String str) {
        return str != null && !str.trim().isEmpty();
    }
    private static String capitalizeFirstLetterAfterSpace(String input) {
        StringBuilder result = new StringBuilder();

        String[] words = input.split(" ");

        for (String word : words) {
            if (!word.isEmpty()) {
                result.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
                result.append(" ");
            }
        }

        if (result.length() > 0) {
            // Remove the trailing space
            result.deleteCharAt(result.length() - 1);
        }

        return result.toString();
    }
}
