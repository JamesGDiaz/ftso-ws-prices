package dev.mouradski.ftso.trades.client.hitbtc;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.ftso.trades.client.AbstractClientEndpoint;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.utils.SymbolHelper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.ClientEndpoint;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
@ClientEndpoint

public class HitbtcClientEndpoint extends AbstractClientEndpoint {

    @Override
    protected String getUri() {
        return "wss://api.hitbtc.com/api/3/ws/public";
    }


    @Override
    protected void subscribe() {
        var pairs = new ArrayList<String>();

        getAssets().stream().map(String::toUpperCase).forEach(base -> getAllQuotesExceptBusd(true).forEach(quote -> pairs.add("\"" + base + quote + "\"")));

        this.sendMessage("{\"method\": \"subscribe\",\"ch\": \"trades\",\"params\": {\"symbols\": [PAIRS],\"limit\": 1},\"id\": ID}".replace("PAIRS", pairs.stream().collect(Collectors.joining(","))).replace("ID", new Date().getTime() + ""));
    }

    @Override
    protected String getExchange() {
        return "hitbtc";
    }

    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {
        var trades = new ArrayList<Trade>();

        var response = gson.fromJson(message, Response.class);

        if (response.getSnapshot() == null) {
            return new ArrayList<>();
        }

        response.getSnapshot().entrySet().forEach(e -> {
            var pair = SymbolHelper.getPair(e.getKey());

            for (HitbtcTrade hitbtcTrade : e.getValue()) {
                trades.add(Trade.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight()).price(hitbtcTrade.getP()).amount(hitbtcTrade.getQ()).build());
            }
        });

        response.getUpdate().entrySet().forEach(e -> {
            var pair = SymbolHelper.getPair(e.getKey());


            for (HitbtcTrade hitbtcTrade : e.getValue()) {
                trades.add(Trade.builder().exchange(getExchange()).base(pair.getLeft()).quote(pair.getRight()).price(hitbtcTrade.getP()).amount(hitbtcTrade.getQ()).timestamp(currentTimestamp()).build());
            }
        });

        return trades;
    }

    @Override
    protected boolean pong(String message) {
        return message.contains("ping");
    }
}
