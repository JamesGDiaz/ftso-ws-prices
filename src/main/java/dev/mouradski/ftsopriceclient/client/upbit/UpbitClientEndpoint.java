package dev.mouradski.ftsopriceclient.client.upbit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.mouradski.ftsopriceclient.utils.Constants;
import dev.mouradski.ftsopriceclient.client.AbstractClientEndpoint;
import dev.mouradski.ftsopriceclient.model.Trade;
import dev.mouradski.ftsopriceclient.service.PriceService;
import dev.mouradski.ftsopriceclient.utils.SymbolHelper;
import jakarta.websocket.ClientEndpoint;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@ClientEndpoint
//X@Component desactive car pas de pairs USD/USDT/USDC
public class UpbitClientEndpoint extends AbstractClientEndpoint {

    private ObjectMapper objectMapper = new ObjectMapper();

    protected UpbitClientEndpoint(PriceService priceSender) {
        super(priceSender);
    }

    @Override
    protected String getUri() {
        return "wss://api.upbit.com/websocket/v1";
    }

    @Override
    protected void subscribe() {

        var pairs = new ArrayList<String>();

        Constants.SYMBOLS.stream().map(String::toUpperCase).forEach(symbol -> {
            Arrays.asList("USD", "USDT", "USDC", "KRW").forEach(quote -> {
                pairs.add("\"" + quote + "-" + symbol + "\"");

            });
        });
        this.sendMessage("[{\"ticket\":\"trades\"},{\"type\":\"trade\",\"codes\":[SYMBOL]}]".replace("SYMBOL", pairs.stream().collect(Collectors.joining(","))));


    }

    @Override
    protected String getExchange() {
        return "upbit";
    }

    @Override
    protected void pong(String message) {
        super.pong(message);
    }

    @Scheduled(fixedDelay = 100 * 1000)
    public void ping() {
        this.sendMessage("PING");
    }

    @Override
    protected List<Trade> mapTrade(String message) throws JsonProcessingException {
        if (!message.contains("trade_price")) {
            return new ArrayList<>();
        }

        var trade = objectMapper.readValue(message, UpbitTrade.class);

        var symbol = SymbolHelper.getQuote(trade.getCode());

        return Arrays.asList(Trade.builder().exchange(getExchange()).symbol(symbol.getLeft()).quote(symbol.getRight()).price(trade.getTradePrice()).amount(trade.getTradeVolume()).build());
    }
}
