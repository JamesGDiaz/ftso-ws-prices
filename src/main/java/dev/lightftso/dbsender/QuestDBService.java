package dev.lightftso.dbsender;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import dev.lightftso.utils.EpochUtils;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.Constants;
import io.quarkus.runtime.Startup;
import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class QuestDBService {
    private String flareNetwork;
    private String questDbIlpHost;
    private Integer questDbIlpPort;

    public LineSenderBuilder senderBuilder;

    public Sender sender;

    private final int bufferCapacity = 1024 * 8;

    protected volatile AtomicInteger tradeCount = new AtomicInteger(0);
    private static AtomicInteger totalTradeCount = new AtomicInteger(0);

    private List<String> ALL_STABLECOINS = Constants.USDT_USDC_BUSD.stream().map(String::toUpperCase).toList();

    private final HashMap<String, Long> tradesPerExchange = new HashMap<>();

    private EpochUtils epochUtils;

    public QuestDBService() {
        Config config = ConfigProvider.getConfig();
        flareNetwork = config.getValue("flare.network", String.class);
        questDbIlpHost = config.getValue("qdb.ilp.host", String.class);
        questDbIlpPort = config.getValue("qdb.ilp.port", Integer.class);
        epochUtils = new EpochUtils(this.flareNetwork);

        

        this.connect();
    }

    @PostConstruct
    void onConstructed() {
        log.info("@ApplicationScoped Questdb constructed");
    }

    @PostConstruct
    public void connect() {
        log.debug("Building QuestDB ILP sender connected to {}:{}", this.questDbIlpHost,
                this.questDbIlpPort);
        sender = Sender.builder().bufferCapacity(this.bufferCapacity).address(questDbIlpHost).port(questDbIlpPort)
                .build();

        var executor = Executors.newSingleThreadScheduledExecutor();
        var epochDelta = epochUtils.getDeltaFromTimestamp(System.currentTimeMillis());

        executor.scheduleAtFixedRate(this::printTradeCount,
                epochDelta + 500, epochUtils.submitDuration,
                TimeUnit.MILLISECONDS);

        executor.scheduleAtFixedRate(() -> {
            log.info("flushing");
            this.sender.flush();
        },
                1, 1,
                TimeUnit.SECONDS);
    }

    public void sendBatch(List<Trade> tradeList) {
        if (tradeList.stream().anyMatch(t -> t.someFieldsAreNull())) {
            return;
        }
        if (tradeList.size() > 1)
            tradeList.sort(Comparator.comparing(Trade::getTimestamp));
        tradeList.forEach(this::writeTradeToBuffer);
        sender.flush();
    }

    public void writeTradeToBuffer(Trade trade) {
        sender.table("trades")
                .symbol("exchange", trade.getExchange())
                .symbol("base", trade.getBase())
                .symbol("quote", trade.getQuote())
                .doubleColumn("price", trade.getPrice())
                .doubleColumn("amount", trade.getAmount())
                .longColumn("epoch", epochUtils.getEpochFromTimestamp(trade.getTimestamp()))
                .boolColumn("stablecoin", isBaseStablecoin(trade.getBase()))
                .at(trade.getTimestamp() * 1000000L);

        tradesPerExchange.putIfAbsent(trade.getExchange(), 0L);
        tradesPerExchange.put(trade.getExchange(), tradesPerExchange.get(trade.getExchange()) + 1);
        totalTradeCount.incrementAndGet();
        System.out.println(totalTradeCount);
    }

    public void send(Trade trade) {
        writeTradeToBuffer(trade);
        sender.flush();
        tradeCount.incrementAndGet();
        totalTradeCount.incrementAndGet();
    }

    private void printTradeCount() {
        var lastEpochId = epochUtils.getLastFinishedEpoch();

        tradesPerExchange.forEach((exc, trds) -> {
            if (trds <= 0) {
                tradesPerExchange.put(exc, 0L);
                log.warn("Didn't receive any trades from {} in epoch {}", exc, lastEpochId);
            }
        });

        var totalTradeCount = QuestDBService.totalTradeCount.getAndSet(0);
        if (totalTradeCount == 0)
            log.error("Received {} trades in epoch {}", totalTradeCount, lastEpochId);
        else
            log.info("Received {} trades in epoch {}", totalTradeCount, lastEpochId);

    }

    protected boolean isBaseStablecoin(String base) {
        return ALL_STABLECOINS.contains(base);
    }

}
