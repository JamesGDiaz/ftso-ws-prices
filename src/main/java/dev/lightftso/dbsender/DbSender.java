package dev.lightftso.dbsender;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dev.mouradski.ftso.trades.model.Trade;
import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;

@Slf4j
public class DbSender {
    @ConfigProperty(name = "questdb_ilp_endpoint", defaultValue = "localhost:9009")
    String questDbIlpEndpoint;

    public LineSenderBuilder senderBuilder;

    public Sender sender;

    private final int bufferCapacity = 1024 * 1024 * 64;

    protected volatile int tradeCount = 0;

    private String exchangeName;

    public DbSender(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public void connect() {
        log.info("Building Quest sender to {}", this.questDbIlpEndpoint);
        senderBuilder = Sender.builder().address("localhost:9009").bufferCapacity(this.bufferCapacity);

        sender = senderBuilder.build();

        var executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::printTradeCount, 1, 1, TimeUnit.SECONDS);
    }

    private void printTradeCount() {
        if (tradeCount <= 0)
            return;

        log.info("{} trades from {}", tradeCount, exchangeName);
        //
        tradeCount = 0;
    }

    public void send(Trade trade) {
        log.info("{} {} {}", trade.getExchange(), trade.getBase(), trade.getQuote());
        sender.table("trades")
                .symbol("exchange", trade.getExchange())
                .symbol("base", trade.getBase())
                .symbol("quote", trade.getQuote())
                .doubleColumn("price", trade.getPrice())
                .doubleColumn("amount", trade.getAmount()).atNow();
        sender.flush();
    }

}