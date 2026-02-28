package com.javarush.countryCache.redis;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Language {

    private String language;
    private Boolean Official;
    private BigDecimal percentage;

    //Getters and Setters omitted
}
