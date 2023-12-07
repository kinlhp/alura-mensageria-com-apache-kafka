package com.kinlhp.learning;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class CustomerControllerServiceMain {

    public static void main(String[] args) throws Exception {
        final var instance = new CustomerControllerServiceMain();
        final var contextHandler = instance.createContext();
        final var server = new Server(8081);
        server.setHandler(contextHandler);
        server.start();
        server.join();
    }

    private ContextHandler createContext() {
        final var context = new ServletContextHandler();
        context.setContextPath("/api/v1");
        context.addServlet(new ServletHolder(new ReportServlet()), "/customers/reports");
        return context;
    }
}
