package dev.lightftso.dbsender;

import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import dev.lightftso.utils.EpochUtils;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.Constants;
import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;

@Slf4j
public class DbSender {
    private String flareNetwork;
    private String questDbIlpHost;
    private Integer questDbIlpPort;

    public LineSenderBuilder senderBuilder;

    public Sender sender;

    private final int bufferCapacity = 1024 * 128;

    protected volatile AtomicInteger tradeCount = new AtomicInteger(0);
    private static AtomicInteger totalTradeCount = new AtomicInteger(0);
    private static AtomicLong cb = new AtomicLong();

    private List<String> ALL_STABLECOINS = Constants.USDT_USDC_BUSD.stream().map(String::toUpperCase).toList();

    private String exchangeName;

    private EpochUtils epochUtils;

    public DbSender(String exchangeName) {
        this.exchangeName = exchangeName;
        Config config = ConfigProvider.getConfig();
        flareNetwork = config.getValue("flare.network", String.class);
        questDbIlpHost = config.getValue("qdb.ilp.host", String.class);
        questDbIlpPort = config.getValue("qdb.ilp.port", Integer.class);
        epochUtils = new EpochUtils(this.flareNetwork);
        cb.set(epochUtils.getRunningEpoch());
    }

    public void connect() {
        log.debug("Building QuestDB for {} sender to {}:{}", this.exchangeName, this.questDbIlpHost,
                this.questDbIlpPort);
        sender = Sender.builder().address(questDbIlpHost).port(questDbIlpPort)
                .bufferCapacity(this.bufferCapacity).build();

        var executor = Executors.newSingleThreadScheduledExecutor();
        var epochDelta = epochUtils.getDeltaFromTimestamp(System.currentTimeMillis());

        executor.scheduleAtFixedRate(this::printTradeCount,
                epochDelta + 500, epochUtils.submitDuration,
                TimeUnit.MILLISECONDS);
    }

    public void sendBatch(Optional<List<Trade>> tradeBatch) {
        if (!tradeBatch.isPresent())
            return;
        try {
            var tradeList = tradeBatch.get();
            tradeList.sort(Comparator.comparing(Trade::getTimestamp));
            tradeList.forEach(this::writeTradeToBuffer);
            sender.flush();
            
            var size = tradeList.size();
            tradeCount.addAndGet(size);
            totalTradeCount.addAndGet(size);
        } catch (NoSuchElementException e) {
        }

    }

    private void writeTradeToBuffer(Trade trade) {
        sender.table("trades")
                .symbol("exchange", trade.getExchange())
                .symbol("base", trade.getBase())
                .symbol("quote", trade.getQuote())
                .doubleColumn("price", trade.getPrice())
                .doubleColumn("amount", trade.getAmount())
                .longColumn("epoch", epochUtils.getEpochFromTimestamp(trade.getTimestamp()))
                .boolColumn("stablecoin", isBaseStablecoin(trade.getBase()))
                .at(trade.getTimestamp() * 1000000L);
    }

    public void send(Trade trade) {
        writeTradeToBuffer(trade);
        sender.flush();
        tradeCount.incrementAndGet();
        totalTradeCount.incrementAndGet();
    }

    private void printTradeCount() {
        var lastEpochId = epochUtils.getLastFinishedEpoch();
        if (tradeCount.getAndSet(0) <= 0) {
            log.warn("Didn't receive any trades from {} in epoch {}", exchangeName, lastEpochId);
        }

        if (cb.getAndSet(epochUtils.getRunningEpoch()) < epochUtils.getRunningEpoch()) {
            var totalTradeCount = DbSender.totalTradeCount.getAndSet(0);
            if (totalTradeCount == 0)
                log.error("Received {} trades in epoch {}", totalTradeCount, lastEpochId);
            else
                log.info("Received {} trades in epoch {}", totalTradeCount, lastEpochId);

        }
    }

    protected boolean isBaseStablecoin(String base) {
        return ALL_STABLECOINS.contains(base);
    }

}