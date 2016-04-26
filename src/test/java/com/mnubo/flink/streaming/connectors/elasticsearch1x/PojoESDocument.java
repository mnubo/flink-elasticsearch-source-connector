package com.mnubo.flink.streaming.connectors.elasticsearch1x;

public class PojoESDocument {
    public String str;
    public boolean boo;
    public long lon;
    public String date;
    public String sub;

    public PojoESDocument() {
    }

    public PojoESDocument(String str, boolean boo, long lon, String date, String sub) {
        this.str = str;
        this.boo = boo;
        this.lon = lon;
        this.date = date;
        this.sub = sub;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PojoESDocument that = (PojoESDocument) o;

        if (boo != that.boo) return false;
        if (lon != that.lon) return false;
        if (str != null ? !str.equals(that.str) : that.str != null) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        return sub != null ? sub.equals(that.sub) : that.sub == null;
    }

    @Override
    public int hashCode() {
        int result = str != null ? str.hashCode() : 0;
        result = 31 * result + (boo ? 1 : 0);
        result = 31 * result + (int) (lon ^ (lon >>> 32));
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (sub != null ? sub.hashCode() : 0);
        return result;
    }
}
