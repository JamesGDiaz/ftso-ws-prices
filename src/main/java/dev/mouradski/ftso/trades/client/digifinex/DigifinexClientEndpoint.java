package dev.mouradski.ftso.trades.client.digifinex;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.ftso.trades.client.AbstractClientEndpoint;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.SymbolHelper;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;

import javax.websocket.ClientEndpoint;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
@ClientEndpoint

public class DigifinexClientEndpoint extends AbstractClientEndpoint {

    @Override
    protected String getUri() {
        return "wss://openapi.digifinex.com/ws/v1/";
    }

    @Override
    protected void subscribe() {
        var markets = getAvailableMarkets();

        var subscriptionMsgTemplate = "{\"method\":\"trades.subscribe\", \"params\":[PAIRS], \"id\":ID}";

        getAssets().stream().map(String::toUpperCase).forEach(base -> {

            if (markets.contains(base + "_USD")) {
                this.sendMessage(subscriptionMsgTemplate.replace("ID", incAndGetIdAsString()).replace("PAIRS",
                        "\"" + base + "_USD\""));
            }

            if (!base.equals("USDT") && markets.contains(base + "_USDT")) {
                this.sendMessage(subscriptionMsgTemplate.replace("ID", incAndGetIdAsString()).replace("PAIRS",
                        "\"" + base + "_USDT\""));
            }

            if (!base.equals("USDC") && markets.contains(base + "_USDC")) {
                this.sendMessage(subscriptionMsgTemplate.replace("ID", incAndGetIdAsString()).replace("PAIRS",
                        "\"" + base + "_USDC\""));
            }
        });
    }

    @Override
    protected String getExchange() {
        return "digifinex";
    }

    @Scheduled(every="20s")
    public void ping() {
        this.sendMessage("{\"method\":\"server.ping\", \"param\":[], \"id\":" + incAndGetIdAsString() + "}");
    }

    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {
        var trades = new ArrayList<Trade>();

        var tradeResponse = gson.fromJson(message, TradeResponse.class);

        if (tradeResponse.getParams() == null) {
            return trades;
        }

        var tradesArray = gson.toJsonTree(tradeResponse.getParams().get(1)).getAsJsonArray();

        var pair = SymbolHelper.getPair(gson.toJsonTree(tradeResponse.getParams().get(2)).getAsString());

        for (var tradeElement : tradesArray) {
            var trade = gson.fromJson(tradeElement, DigifinexTrade.class);

            trades.add(Trade.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight())
                    .price(trade.getPrice()).amount(trade.getAmount()).timestamp(currentTimestamp()).build());
        }

        return trades;
    }

    private Set<String> getAvailableMarkets() {
        var client = HttpClient.newHttpClient();

        var request = HttpRequest.newBuilder()
                .uri(URI.create("https://openapi.digifinex.com/v3/markets"))
                .header("Content-Type", "application/json")
                .GET()
                .build();

        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());

            var marketData = gson.fromJson(response.body(), MarketData.class);

            return marketData.getData().stream().map(MarketInfo::getMarket).map(String::toUpperCase)
                    .collect(Collectors.toSet());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return new HashSet<>();
    }

}
