package ingestion_service;

import io.javalin.http.Context;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class IngestionController {

    public static void ingestBook(Context ctx) throws IOException {
        String bookId = ctx.pathParam("book_id");
        String apiUrl = "https://gutendex.com/books/" + bookId;

        HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
        conn.setRequestMethod("GET");

        String jsonResponse = new String(conn.getInputStream().readAllBytes());
        var bookInfo = BookParser.extractBookInfo(jsonResponse);

        // Save metadata and body text
        String folder = "datalake/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd/HH"));
        Files.createDirectories(Paths.get(folder));
        Files.writeString(Paths.get(folder, bookId + "_header.txt"), jsonResponse);
        // Example: dummy body, later fetch full text
        Files.writeString(Paths.get(folder, bookId + "_body.txt"), "Body of book " + bookId);

        ctx.json(Map.of("status", "success", "book_id", bookId));
    }

    public static void checkStatus(Context ctx) {
        String bookId = ctx.pathParam("book_id");
        Path header = Paths.get("datalake").resolve(bookId + "_header.txt");
        ctx.json(Map.of("book_id", bookId, "exists", Files.exists(header)));
    }

    public static void listBooks(Context ctx) throws IOException {
        Path datalake = Paths.get("datalake");
        if (!Files.exists(datalake)) {
            ctx.json(List.of());
            return;
        }
        try (var stream = Files.walk(datalake)) {
            List<String> files = stream.filter(Files::isRegularFile)
                                       .map(p -> p.getFileName().toString())
                                       .toList();
            ctx.json(files);
        }
    }
}
