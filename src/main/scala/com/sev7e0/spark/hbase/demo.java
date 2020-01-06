package com.sev7e0.spark.hbase;

public enum demo {
    A,B,C,D,E
}

class d{
    public static void main(String[] args) {
        demo[] values = demo.values();
        System.out.println(values.length);
        System.out.println(values[-1]);

    }


}