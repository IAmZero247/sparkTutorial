package com.sparkTutorial.mylearning.dataset.javabeans;

import java.io.Serializable;

public class Segment implements Serializable {
    private Point from;
    private Point to;

    public Segment(Point from,  Point to) {
        this.to = to;
        this.from = from;
    }

    public Point getFrom() {
        return from;
    }

    public void setFrom(Point from) {
        this.from = from;
    }

    public Point getTo() {
        return to;
    }

    public void setTo(Point to) {
        this.to = to;
    }
}
