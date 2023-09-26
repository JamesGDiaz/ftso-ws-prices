package dev.mouradski.ftso.trades.server;


import dev.mouradski.ftso.trades.model.Trade;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.server.ServerEndpoint;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

@ServerEndpoint("/trade")
@Slf4j
@ApplicationScoped
@Startup
public class TradeServer extends WsServer<Trade> {
    protected static final CopyOnWriteArrayList<Trade> test = new CopyOnWriteArrayList<Trade>();
    protected static final Set<WsServer<Trade>> listeners = new CopyOnWriteArraySet<>();

    @Override
    protected Set<WsServer<Trade>> getListeners() {
        return listeners;
    }
}
