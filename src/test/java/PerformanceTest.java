package com.javarush.countryCache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import lombok.extern.slf4j.Slf4j;
import com.javarush.countryCache.dao.CityDAO;
import com.javarush.countryCache.domain.City;
import com.javarush.countryCache.domain.Country;
import com.javarush.countryCache.domain.CountryLanguage;
import com.javarush.countryCache.redis.CityCountry;
import com.javarush.countryCache.redis.Language;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
class PerformanceTest {

    private static SessionFactory sessionFactory;
    private static CityDAO cityDAO;
    private static RedisClient redisClient;
    private static ObjectMapper mapper;
    private static List<Integer> testIds = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

    @BeforeAll
    static void setup() {
        sessionFactory = prepareRelationalDb();
        cityDAO = new CityDAO(sessionFactory);

        redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            log.info("Redis connection established");
        }

        mapper = new ObjectMapper();

        List<City> cities;
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            cities = session.createQuery(
                            "select distinct c " +
                                    "from City c " +
                                    "join fetch c.country co " +
                                    "join fetch co.languages",
                            City.class
                    )
                    .getResultList();
            session.getTransaction().commit();
        }

        List<CityCountry> cityCountries = transformData(cities);
        pushToRedis(cityCountries);
    }

    @AfterAll
    static void tearDown() {
        if (sessionFactory != null && !sessionFactory.isClosed()) {
            sessionFactory.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    void testRedisPerformance() {
        long start = System.currentTimeMillis();
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (Integer id : testIds) {
                String json = sync.get("city:" + id);
                assertNotNull(json, "City data for " + id + " was not found in Redis");
                mapper.readValue(json, CityCountry.class);
            }
        } catch (JsonProcessingException e) {
            log.error("Error " + e);
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Redis read 10 cities: " + duration + " ms");
    }

    @Test
    void testMySqlPerformance() {
        long start = System.currentTimeMillis();
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();

            for (Integer id : testIds) {
                City city = session.get(City.class, id);
                assertNotNull(city, "City with id " + id + " was not found in MySQL");
                city.getCountry().getLanguages().size();
            }

            session.getTransaction().commit();
        }
        long duration = System.currentTimeMillis() - start;
        log.info("MySQL read 10 cities: " + duration + " ms");
    }

    private static void pushToRedis(List<CityCountry> data) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> redisStringCommands = connection.sync();
            for (CityCountry cityCountry : data) {
                String key = "city:" + cityCountry.getId();                 // city:123
                String value = mapper.writeValueAsString(cityCountry);      // JSON-строка
                redisStringCommands.set(key, value);
            }
        } catch (JsonProcessingException e) {
            log.error("Error in Redis" + e);
        }
    }

    private static List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry res = new CityCountry();
            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();
            res.setAlternativeCountryCode(country.getAlternativeCode());
            res.setContinent(country.getContinent());
            res.setCountryCode(country.getCode());
            res.setCountryName(country.getName());
            res.setCountryPopulation(country.getPopulation());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());

            Set<CountryLanguage> cls = country.getLanguages();
            Set<Language> languages = cls.stream().map(cl -> {
                Language lang = new Language();
                lang.setLanguage(cl.getLanguage());
                lang.setOfficial(cl.getOfficial());
                lang.setPercentage(cl.getPercentage());
                return lang;
            }).collect(Collectors.toSet());

            res.setLanguages(languages);
            return res;
        }).collect(Collectors.toList());
    }

    private static SessionFactory prepareRelationalDb() {
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        return new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
    }
}
