package edu.reduce.map.fun;


import org.apache.hadoop.io.Text;

public enum Player {
    BLACK(new Text("black")),
    WHITE(new Text("white"));

    final Text value;

    Player(Text value) {
        this.value = value;
    }

    public Text getValue() {
        return value;
    }
}
