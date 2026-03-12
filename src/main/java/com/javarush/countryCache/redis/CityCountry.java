package com.javarush.countryCache.redis;

import com.javarush.countryCache.domain.Continent;
import lombok.Data;


import java.math.BigDecimal;
import java.util.Set;

@Data
public class CityCountry {

    private Integer id;

    private String name;

    private String district;

    private Integer population;

    private String countryCode;

    private String alternativeCountryCode;

    private String countryName;

    private Continent continent;

    private String countryRegion;

    private BigDecimal countrySurfaceArea;

    private Integer countryPopulation;

    private Set<Language> languages;

    //Getters and Setters omitted
}
