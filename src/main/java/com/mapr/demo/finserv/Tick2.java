package com.mapr.demo.finserv;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.Serializable;

/**
 * This tick is a data structure containing a single tick
 * that avoids parsing the underlying bytes as long as possible.
 * <p>
 * By using annotations, it also supports fast serialization to JSON.
 */
public class Tick2 implements Serializable {
    private byte[] data;

    public Tick2(byte[] data) {
        this.data = data;
    }

    public Tick2(String data) {
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
        return new String(data, 10, 6).trim();
    }

    @JsonProperty("symbol-suffix")
    String getSymbolSuffix() {
        return new String(data, 16, 10).trim();
    }

    @JsonProperty("sale-condition")
    String getSaleCondition() {
        return new String(data, 26, 4).trim();
    }

    @JsonProperty("trade-volume")
    String getTradeVolume() {
        int i = 30;
        while (i < 39 && data[i] == '0') {
            i++;
        }
        return new String(data, i, 39 - i);
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

    String getTradeReportingFacility() {
        return new String(data, 70, 1);
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
            throw new IOException("Couldn't read entire Tick2 object, only got " + n + " bytes");
        }

    }
}
