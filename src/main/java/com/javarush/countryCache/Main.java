package com.javarush.countryCache;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.countryCache.dao.CityDAO;
import com.javarush.countryCache.dao.CountryDAO;

import com.javarush.countryCache.domain.City;
import com.javarush.countryCache.domain.Country;
import com.javarush.countryCache.domain.CountryLanguage;
import com.javarush.countryCache.redis.CityCountry;
import com.javarush.countryCache.redis.Language;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;

    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public static void main(String[] args) {
        Main app = new Main();
        try {
            app.run();
        } finally {
            app.shutdown();
        }
    }

    public Main() {
        sessionFactory = prepareRelationalDb();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }

    public void run() {
        List<CityCountry> preparedData = new ArrayList<>();

        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                int totalCount = cityDAO.getTotalCount(session);

                int step = 500;
                for (int offset = 0; offset < totalCount; offset += step) {
                    List<City> batch = cityDAO.getItems(session, offset, step);
                    preparedData.addAll(transformData(batch));

                    // чтобы память не росла при больших объёмах
                    session.clear();
                }



                tx.commit();
            } catch (Exception e) {
                if (tx != null && tx.isActive()) {
                    tx.rollback();
                }
                throw e;
            }
        }

        // Redis уже после DB транзакции (мы работаем с DTO, не с lazy-сущностями)
        pushToRedis(preparedData);
    }

    private RedisClient prepareRedisClient() {
        RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            System.out.println("Connected to Redis");
        }
        return client;
    }
    private List<CityCountry> transformData(List<City> cities) {
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

            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(cl -> {
                Language language = new Language();
                language.setLanguage(cl.getLanguage());
                language.setOfficial(cl.getOfficial());
                language.setPercentage(cl.getPercentage());
                return language;
            }).collect(Collectors.toSet());

            res.setLanguages(languages);
            return res;
        }).collect(Collectors.toList());
    }

    private void pushToRedis(List<CityCountry> data) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();

            for (CityCountry cityCountry : data) {
                try {
                    sync.set(String.valueOf(cityCountry.getId()), mapper.writeValueAsString(cityCountry));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private SessionFactory prepareRelationalDb() {
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        // ВАЖНО: если ты используешь openSession(), то thread-context НЕ нужен:
        // properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");

        return new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) sessionFactory.close();
        if (nonNull(redisClient)) redisClient.shutdown();
    }
}

























