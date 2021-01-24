package com.sparkTutorial.mylearning.dataset.javabeans;

import java.io.Serializable;

public class NumberEngToFre  implements Serializable {

    private int i;
    private String english;
    private String french;

    public NumberEngToFre(int i, String english, String french) {
        this.i = i;
        this.english = english;
        this.french = french;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public String getEnglish() {
        return english;
    }

    public void setEnglish(String english) {
        this.english = english;
    }

    public String getFrench() {
        return french;
    }

    public void setFrench(String french) {
        this.french = french;
    }
}
