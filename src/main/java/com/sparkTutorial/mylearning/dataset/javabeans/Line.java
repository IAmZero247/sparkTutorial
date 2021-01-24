package com.sparkTutorial.mylearning.dataset.javabeans;

import java.io.Serializable;

public class Line implements Serializable {
    private String name;
    private Point[] points;

    public Line(String name, Point[] points) {
        this.name = name;
        this.points = points;
    }

    public String getName() { return name; }

    public void setName(String name) { this.name = name; }

    public Point[] getPoints() { return points; }

    public void setPoints(Point[] points) { this.points = points; }
}

