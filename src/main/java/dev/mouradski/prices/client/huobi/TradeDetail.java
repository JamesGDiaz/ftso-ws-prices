package dev.mouradski.prices.client.huobi;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TradeDetail {
    @SerializedName("id")
    private String id;

    @SerializedName("ts")
    private String ts;

    @SerializedName("tradeId")
    private long tradeId;

    @SerializedName("amount")
    private double amount;

    @SerializedName("price")
    private double price;

    @SerializedName("direction")
    private String direction;
}
