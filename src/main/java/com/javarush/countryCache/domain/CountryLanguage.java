package com.javarush.countryCache.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Type;

import java.math.BigDecimal;

@Entity
@Table(schema = "world", name = "country_language")
@Getter
@Setter
@ToString(exclude = {"country"})
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CountryLanguage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    @EqualsAndHashCode.Include
    private Integer id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "country_id")
    private Country country;

    private String language;

    @Column(name = "is_official", columnDefinition = "BIT")
    @Type(type = "org.hibernate.type.NumericBooleanType")
    private Boolean official;

    private BigDecimal percentage;
}









