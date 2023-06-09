package dev.mouradski.ftso.trades.client.btcex;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.ftso.trades.client.AbstractClientEndpoint;
import dev.mouradski.ftso.trades.model.Trade;
import dev.mouradski.ftso.trades.service.PriceService;
import dev.mouradski.ftso.trades.utils.SymbolHelper;
import jakarta.websocket.ClientEndpoint;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ClientEndpoint
public class BtcexClientEndpoint extends AbstractClientEndpoint {

    protected BtcexClientEndpoint(PriceService priceSender, @Value("${exchanges}") List<String> exchanges, @Value("${assets}") List<String> assets) {
        super(priceSender, exchanges, assets);
    }

    @Override
    protected String getUri() {
        return "wss://api.btcex.com/ws/api/v1";
    }

    @Override
    protected void subscribe() {
        getAssets(true).stream().filter(symbol -> !getAllQuotes(true).contains(symbol)).forEach(symbol -> {
                var msg = "{\"jsonrpc\" : \"2.0\",\"id\" : ID,\"method\" : \"/public/subscribe\",\"params\" : {\"channels\":[\"trades.SYMBOL-USDT-SPOT.raw\"]}}"
                        .replace("ID", counter.getCount().toString()).replace("SYMBOL", symbol);
                this.sendMessage(msg);
        });
    }

    @Override
    protected String getExchange() {
        return "btcex";
    }


    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {
        if (!message.contains("trade_id")) {
            return new ArrayList<>();
        }

        var tradeResponse = objectMapper.readValue(message, TradeResponse.class);

        var trades = new ArrayList<Trade>();

        tradeResponse.getParams().getData().forEach(tradeData -> {
            Pair<String, String> symbol = SymbolHelper.getSymbol(tradeData.getInstrumentName().replace("-SPOT", ""));

            trades.add(Trade.builder().exchange(getExchange()).symbol(symbol.getLeft()).quote(symbol.getRight()).price(tradeData.getPrice()).amount(tradeData.getAmount()).build());
        });

        return trades;
    }

    @Scheduled(fixedDelay = 15000)
    public void ping() {
        this.sendMessage("{ \"jsonrpc\":\"2.0\",\"id\": ID,\"method\": \"/public/ping\",\"params\":{}}".replace("ID", counter.getCount().toString()));
    }
}
