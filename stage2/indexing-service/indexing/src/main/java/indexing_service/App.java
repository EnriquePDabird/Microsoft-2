package indexing_service;

import io.javalin.Javalin;

public class App {
    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7002);
        
        app.post("/index/rebuild", IndexController::rebuildIndex);
        app.get("/index/status", IndexController::status);
    }
}