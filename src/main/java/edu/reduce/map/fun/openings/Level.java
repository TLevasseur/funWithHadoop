package edu.reduce.map.fun.openings;

public enum Level {
    GOOD,
    OK,
    BAD;

    public static Level from(int elo) {
        if (elo > 1700) {
            return GOOD;
        } else if (elo < 1200) {
            return BAD;
        } else {
            return OK;
        }
    }
}
