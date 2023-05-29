package dev.mouradski.ftsopriceclient.client.hitbtc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import dev.mouradski.ftsopriceclient.utils.Constants;
import dev.mouradski.ftsopriceclient.client.AbstractClientEndpoint;
import dev.mouradski.ftsopriceclient.model.Trade;
import dev.mouradski.ftsopriceclient.service.PriceService;
import dev.mouradski.ftsopriceclient.utils.SymbolHelper;
import jakarta.websocket.ClientEndpoint;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@ClientEndpoint
@Component
public class HitbtcClientEndpoint extends AbstractClientEndpoint {

    protected HitbtcClientEndpoint(PriceService priceSender) {
        super(priceSender);
    }

    @Override
    protected String getUri() {
        return "wss://api.hitbtc.com/api/3/ws/public";
    }



    @Override
    protected void subscribe() {
        var pairs = new ArrayList<String>();

        Constants.SYMBOLS.stream().map(String::toUpperCase).forEach(symbol -> {
            getAllQuotesExceptBusd(true).forEach(quote -> {
                pairs.add("\"" + symbol + quote + "\"");
            });

        });

        this.sendMessage("{\"method\": \"subscribe\",\"ch\": \"trades\",\"params\": {\"symbols\": [PAIRS],\"limit\": 1},\"id\": ID}".replace("PAIRS", pairs.stream().collect(Collectors.joining(","))).replace("ID", new Date().getTime() + ""));
    }

    @Override
    protected String getExchange() {
        return "hitbtc";
    }

    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {
        var trades = new ArrayList<Trade>();

        var gson = new Gson();
        var response = gson.fromJson(message, Response.class);

        if (response.getSnapshot() == null) {
            return new ArrayList<>();
        }

        response.getSnapshot().entrySet().forEach(e -> {
            var symbol = SymbolHelper.getQuote(e.getKey());

            for (HitbtcTrade hitbtcTrade : e.getValue()) {
                trades.add(Trade.builder().exchange(getExchange()).symbol(symbol.getLeft()).quote(symbol.getRight()).price(hitbtcTrade.getP()).amount(hitbtcTrade.getQ()).build());
            }
        });

        response.getUpdate().entrySet().forEach(e -> {
            var symbol = SymbolHelper.getQuote(e.getKey());


            for (HitbtcTrade hitbtcTrade : e.getValue()) {
                trades.add(Trade.builder().exchange(getExchange()).symbol(symbol.getLeft()).quote(symbol.getRight()).price(hitbtcTrade.getP()).amount(hitbtcTrade.getQ()).build());
            }
        });

        return trades;
    }
}
