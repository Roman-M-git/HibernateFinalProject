package com.javarush.countryCache.domain;
import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Set;

@Entity
@Table(schema = "world", name = "country")
@Data
    public class Country {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "id") // может должно быть "country_id"?
        private Integer id;

        private String code;

        @Column(name = "code_2")
        private String alternativeCode;

        private String name;

        @Column(name = "continent")
        @Enumerated(EnumType.ORDINAL)
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
        private BigDecimal GNP;

        @Column(name = "gnpo_id")
        private BigDecimal GNPOId;

        @Column(name = "local_name")
        private String localName;

        @Column(name = "government_form")
        private String governmentForm;

        @Column(name = "head_of_state")
        private String headOfState;

        @OneToOne
        @JoinColumn(name = "capital")
        private City city;

        @OneToMany(fetch = FetchType.EAGER)
        @JoinColumn(name = "country_id")
        private Set<CountryLanguage> languages;


        //Getters and Setters omitted

}

