# FTSO-WS-PRICES
Collect the prices of the following assets in real time: "xrp", "btc", "eth", "algo", "xlm", "ada", "matic", "sol", "fil", "flr", "sgb", "doge", "xdc", "arb", "avax", "bnb", "usdc", "busd", "usdt", "dgb", "bch"

The list of supported exchanges will evolve over time.


## Run with Docker

Edit the .env file and define the assets you want to collect prices in real time (in this example all supported exchanges)

```sh
ASSETS=xrp,btc,eth,algo,xlm,ada,matic,sol,fil,flr,sgb,doge,xdc,arb,avax,bnb,usdc,busd,usdt
EXCHANGES=binance,binanceus,bitfinex,bitrue,bitstamp,bybit,cex,coinbase,crypto,digifinex,fmfw,gateio,hitbtc,huobi,kraken,kucoin,lbank,mexc,okex,upbit,btcex
```

Run container 

```sh
docker-compose up
```

## Connect to WS
ws://localhost:8985/trade

## Use as library in a spring-boot app 

Build dependency 

```sh
mvn clean install
```

Add in pom.xml 

```xml
    <dependency>
        <groupId>dev.mouradski</groupId>
        <artifactId>ftso-price-client</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
```

Scan packages 

```java
@SpringBootApplication(scanBasePackages = {"dev.mouradski.ftsopriceclient", "other.base.package"})
```

implement TradeConsummer interface

```java
import dev.mouradski.ftsopriceclient.service.TradeConsummer;
import dev.mouradski.ftsopriceclient.model.Trade;
import org.springframework.stereotype.Component;

@Component
public class TradeConsummerImpl implements TradeConsummer {

    public void processTrade(Trade trade) {
        //TODO do what you want with this trade
    }
}
```

Add this property to your application.properties to disable serving websocket

```properties
serve.websocket=false
```


