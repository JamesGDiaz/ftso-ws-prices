package dev.mouradski.prices.client.okex;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.mouradski.prices.client.AbstractClientEndpoint;
import dev.mouradski.prices.model.Trade;
import dev.mouradski.prices.service.PriceService;
import dev.mouradski.prices.utils.SymbolHelper;
import jakarta.websocket.ClientEndpoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ClientEndpoint
@Component
public class OkexClientEndpoint extends AbstractClientEndpoint {
    protected OkexClientEndpoint(PriceService priceSender, @Value("${exchanges}") List<String> exchanges, @Value("${assets}") List<String> assets) {
        super(priceSender, exchanges, assets);
    }

    @Override
    protected String getUri() {
        return "wss://ws.okx.com:8443/ws/v5/public";
    }

    @Override
    protected void subscribe() {

        var subscriptionMsg = new StringBuilder("{\"op\": \"subscribe\",\"args\": [");

        var channels = new ArrayList<String>();

        getAssets(true).forEach(symbol -> {
            getAllQuotesExceptBusd(true).forEach(quote -> {
                channels.add("{\"channel\": \"trades\",\"instId\": \"SYMBOL-QUOTE\"}".replace("SYMBOL", symbol).replace("QUOTE", quote));
            });
        });

        subscriptionMsg.append(channels.stream().collect(Collectors.joining(",")));
        subscriptionMsg.append("]}");

        this.sendMessage(subscriptionMsg.toString());

    }

    @Override
    protected String getExchange() {
        return "okex";
    }

    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {

        if (!message.contains("\"channel\":\"trades\"")) {
            return new ArrayList<>();
        }

        var tradeData = objectMapper.readValue(message, TradeData.class);

        var symbol = SymbolHelper.getSymbol(tradeData.getArg().getInstId());

        var trades = new ArrayList<Trade>();

        tradeData.getData().forEach(data -> {
            trades.add(Trade.builder().exchange(getExchange()).symbol(symbol.getLeft()).quote(symbol.getRight()).price(data.getPx()).amount(data.getSz()).build());
        });

        return trades;
    }

    @Scheduled(fixedDelay = 30 * 1000)
    public void ping() {
        this.sendMessage("ping");
    }
}
