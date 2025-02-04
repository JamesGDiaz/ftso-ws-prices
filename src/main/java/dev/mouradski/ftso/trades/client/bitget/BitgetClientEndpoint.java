package dev.mouradski.ftso.trades.client.bitget;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.ftso.trades.client.AbstractClientEndpoint;
import dev.mouradski.ftso.trades.model.Ticker;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.SymbolHelper;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.ClientEndpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
@ClientEndpoint
@Startup
public class BitgetClientEndpoint extends AbstractClientEndpoint {

    private boolean subscribed;

    @Override
    protected String getUri() {
        return "wss://ws.bitget.com/spot/v1/stream";
    }

    @Override
    protected void subscribeTrade() {
        subscribeTradeTickers();
    }

    @Override
    protected void subscribeTicker() {
        subscribeTradeTickers();
    }
    private void subscribeTradeTickers() {
        if (subscribed) {
            return;
        }
        subscribed = true;

        var args = new ArrayList<String>();


        getAssets(true).forEach(base -> getAllQuotesExceptBusd(true).forEach(quote -> {
            if (subscribeTrade) {
                args.add("{\"instType\": \"sp\",\"channel\": \"trade\",\"instId\": \"PAIR\"}".replace("PAIR", base + quote));
            }

            if (subscribeTicker) {
                args.add("{\"instType\": \"sp\",\"channel\": \"ticker\",\"instId\": \"PAIR\"}".replace("PAIR", base + quote));
            }

        }));



        this.sendMessage("{\"op\": \"subscribe\",\"args\": [ARGS]}".replace("ARGS", String.join(",", args)));
    }



    @Override
    protected Optional<List<Ticker>> mapTicker(String message) throws JsonProcessingException {
        if (!message.contains("open24h")) {
            return Optional.empty();
        }

        var tickerResponse = objectMapper.readValue(message, TickerResponse.class);

        var tickers = new ArrayList<Ticker>();

        var pair = SymbolHelper.getPair(tickerResponse.getArg().getInstId());

        for (var ticker : tickerResponse.getData()) {
            tickers.add(Ticker.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight()).lastPrice(ticker.getLast()).timestamp(currentTimestamp()).build());
        }

        return Optional.of(tickers);
    }

    @Override
    protected String getExchange() {
        return "bitget";
    }


    @Override
    protected Optional<List<Trade>> mapTrade(String message) throws JsonProcessingException {

        if (message.contains("error") || !message.contains("update")) {
            return Optional.empty();
        }

        var trades = new ArrayList<Trade>();

        var update = this.objectMapper.readValue(message, Update.class);

        var pair = SymbolHelper.getPair(update.getArg().getInstId());


        update.getData().forEach(tradeData -> {
            trades.add(Trade.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight()).amount(tradeData.getQuantity()).price(tradeData.getPrice()).timestamp(currentTimestamp()).build());
        });

        return Optional.of(trades);
    }

    @Scheduled(every = "30s")
    public void ping() {
        this.sendMessage("ping");
    }
}
