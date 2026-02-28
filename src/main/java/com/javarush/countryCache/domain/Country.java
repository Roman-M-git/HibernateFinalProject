package com.javarush.countryCache.domain;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(schema = "world", name = "country")
@Getter
@Setter
@ToString(exclude = {"city", "languages"})
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Country {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    @EqualsAndHashCode.Include
    private Integer id;

    private String code;

    @Column(name = "code_2")
    private String alternativeCode;

    private String name;

    @Column(name = "continent")
    @Enumerated(EnumType.ORDINAL) // можно заменить на STRING, если в БД enum текстовый
    private Continent continent;

    private String region;

    @Column(name = "surface_area")
    private BigDecimal surfaceArea;

    @Column(name = "indep_year")
    private Short independenceYear;

    private Integer population;

    @Column(name = "life_expectancy")
    private BigDecimal lifeExpectancy;

    @Column(name = "gnp")
    private BigDecimal gnp;

    @Column(name = "gnpo_id")
    private BigDecimal gnpoId;

    @Column(name = "local_name")
    private String localName;

    @Column(name = "government_form")
    private String governmentForm;

    @Column(name = "head_of_state")
    private String headOfState;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "capital")
    private City city;

    // ВАЖНО: mappedBy, а не JoinColumn здесь!
    @OneToMany(mappedBy = "country", fetch = FetchType.LAZY)
    private Set<CountryLanguage> languages = new HashSet<>();
}