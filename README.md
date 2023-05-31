# FTSO-WS-PRICES
Collect the prices of the following assets in real time: "xrp", "btc", "eth", "algo", "xlm", "ada", "matic", "sol", "fil", "flr", "sgb", "doge", "xdc", "arb", "avax", "bnb", "usdc", "busd", "usdt", "dgb", "bch"

The list of supported exchanges will evolve over time.


## Run with Docker

Edit the .env file and define the assets you want to collect prices in real time (in this example all supported exchanges)

```sh
ASSETS=xrp,btc,eth,algo,xlm,ada,matic,sol,fil,flr,sgb,doge,xdc,arb,avax,bnb,usdc,busd,usdt
EXCHANGES=binance,binanceus,bitfinex,bitrue,bitstamp,bybit,cex,coinbase,crypto,digifinex,fmfw,gateio,hitbtc,huobi,kraken,kucoin,lbank,mexc,okex,upbit
```

Run container 

```sh
docker-compose up
```

## Connect to WS
ws://localhost:8985/trade
