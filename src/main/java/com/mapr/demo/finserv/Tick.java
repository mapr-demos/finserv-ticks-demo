package com.mapr.demo.finserv;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This tick is a data structure containing a single tick
 * that avoids parsing the underlying bytes as long as possible.
 * <p>
 * By using annotations, it also supports fast serialization to JSON.
 */
public class Tick implements Serializable {
    private byte[] data;

    public Tick(byte[] data) {
        this.data = data;
    }

    public Tick(String data) {
        this.data = data.getBytes(Charsets.ISO_8859_1);
    }

    @JsonProperty("date")
    String getDate() {
        return new String(data, 0, 9);
    }

    @JsonProperty("exchange")
    String getExchange() {
        return new String(data, 9, 1);
    }

    @JsonProperty("symbol-root")
    String getSymbolRoot() {
        return trim(10, 6);
    }

    @JsonProperty("symbol-suffix")
    String getSymbolSuffix() {
        return trim(16, 10);
    }

    @JsonProperty("sale-condition")
    String getSaleCondition() {
        return trim(26, 4);
    }

    @JsonProperty("trade-volume")
    double getTradeVolume() {
        return digitsAsInt(30, 9);
    }

    @JsonProperty("trade-price")
    double getTradePrice() {
        return digitsAsDouble(39, 11, 4);
    }

    //String getTradePrice() {return new String(data, 39, 46) + "." + record.substring(data, 46, getTradeStopStockIndicator() {return new String(data, 50, 51); }

    @JsonProperty("trade-correction-indicator")
    String getTradeCorrectionIndicator() {
        return new String(data, 51, 2);
    }

    @JsonProperty("trade-sequence-number")
    String getTradeSequenceNumber() {
        return new String(data, 53, 16);
    }

    @JsonProperty("trade-source")
    String getTradeSource() {
        return new String(data, 69, 1);
    }

    @JsonProperty("trade-reporting-facility")
    String getTradeReportingFacility() {
        return new String(data, 70, 1);
    }

    @JsonProperty("sender")
    String getSender() {
        return new String(data,71,4);
    }

    @JsonProperty("receiver-list")
    List<String> getReceivers() {
        List<String> receivers = new ArrayList<>();
        for (int i=0; data.length >= 78 + i*4; i++) {
            receivers.add(new String(data, 75 + i*4, 4));
        }
        return receivers;
    }

    private double digitsAsDouble(int start, int length, int decimals) {
        double r = digitsAsInt(start, length);
        for (int i = 0; i < decimals; i++) {
            r = r / 10;
        }
        return r;
    }

    private int digitsAsInt(int start, int length) {
        int r = 0;
        for (int i = start; i < start + length; i++) {
            if (data[i] != ' ') {
                r = r * 10 + data[i] - '0';
            }
        }
        return r;
    }

    private String trim(int start, int length) {
        int i = start;
        while (i < start + length && data[i] == ' ') {
            i++;
        }
        return new String(data, i, start + length - i);
    }

    public void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    public void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        int length = in.readInt();
        data = new byte[length];
        int n = in.read(data);
        if (n != length) {
            throw new IOException("Couldn't read entire Tick object, only got " + n + " bytes");
        }

    }
}
