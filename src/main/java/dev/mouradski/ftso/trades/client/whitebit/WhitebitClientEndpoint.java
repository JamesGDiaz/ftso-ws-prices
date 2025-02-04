package dev.mouradski.ftso.trades.client.whitebit;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.ftso.trades.client.AbstractClientEndpoint;
import dev.mouradski.ftso.trades.model.Ticker;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.SymbolHelper;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.ClientEndpoint;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
@ClientEndpoint
@Slf4j
@Startup
public class WhitebitClientEndpoint extends AbstractClientEndpoint {

    private Set<String> supportedSymbols;

    @Override
    protected String getUri() {
        return "wss://api.whitebit.com/ws";
    }

    @Override
    protected void subscribeTrade() {
        var pairs = new ArrayList<String>();

        getAssets(true).forEach(base -> getAllQuotesExceptBusd(true).forEach(quote -> {
            var pair = base + "_" + quote;

            if (supportedSymbols.contains(pair)) {
                pairs.add("\"" + pair + "\"");
            }
        }));

        this.sendMessage("{\"id\": ID,\"method\": \"trades_subscribe\",\"params\": [PAIRS]}"
                .replace("ID", incAndGetIdAsString())
                .replace("PAIRS", String.join(",", pairs)));

    }

    @Override
    protected void subscribeTicker() {
        var pairs = new ArrayList<String>();

        getAssets(true).forEach(base -> getAllQuotesExceptBusd(true).forEach(quote -> {
            var pair = base + "_" + quote;

            if (supportedSymbols.contains(pair)) {
                pairs.add("\"" + pair + "\"");
            }
        }));

        this.sendMessage("{\"id\": ID,\"method\": \"lastprice_subscribe\",\"params\": [PAIRS]}"
                .replace("ID", incAndGetIdAsString())
                .replace("PAIRS", String.join(",", pairs)));

    }

    @Override
    protected Optional<List<Ticker>> mapTicker(String message) throws JsonProcessingException {
        if (!message.contains("lastprice_update")) {
            return Optional.empty();
        }

        var priceUpdate = objectMapper.readValue(message, PriceUpdate.class);

        var pair = SymbolHelper.getPair(priceUpdate.getSymbol());

        return Optional.of(Collections.singletonList(Ticker.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight()).lastPrice(priceUpdate.getLastPrice()).timestamp(currentTimestamp()).build()));
    }

    @Override
    protected String getExchange() {
        return "whitebit";
    }


    @Override
    protected Optional<List<Trade>> mapTrade(String message) throws JsonProcessingException {
        if (!message.contains("trades_update")) {
            return Optional.empty();
        }

        var trades = new ArrayList<Trade>();


        var tradeUpdateMessage = this.objectMapper.readValue(message, TradeUpdateMessage.class);

        var pair = SymbolHelper.getPair(tradeUpdateMessage.getParams().get(0).toString());

        ((List<Map>) tradeUpdateMessage.getParams().get(1)).stream()
                .sorted(Comparator.comparing(v -> v.get("time").toString())).forEach(tradeUpdate -> trades.add(Trade.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight())
                        .price(Double.valueOf(tradeUpdate.get("price").toString())).amount(Double.valueOf(tradeUpdate.get("amount").toString())).timestamp(currentTimestamp()).build()));

        return Optional.of(trades);

    }

    @Override
    protected void prepareConnection() {
        var client = HttpClient.newHttpClient();

        var request = HttpRequest.newBuilder()
                .uri(URI.create("https://whitebit.com/api/v4/public/markets"))
                .header("Content-Type", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            var markets = gson.fromJson(response.body(), MarketPair[].class);

            this.supportedSymbols = Stream.of(markets).map(MarketPair::getName).collect(Collectors.toSet());

        } catch (IOException | InterruptedException e) {
            log.error("Caught exception collecting markets from {}", getExchange());
        }
    }

    @Scheduled(every="30s")
    public void ping() {
        this.sendMessage("{\"id\": ID,\"method\": \"ping\",\"params\": []}".replace("ID", incAndGetIdAsString()));
    }
}
