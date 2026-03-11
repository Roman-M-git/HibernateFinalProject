package com.javarush.countryCache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import com.javarush.countryCache.dao.CityDAO;
import com.javarush.countryCache.dao.CountryDAO;
import com.javarush.countryCache.domain.City;
import com.javarush.countryCache.domain.Country;
import com.javarush.countryCache.domain.CountryLanguage;
import com.javarush.countryCache.redis.CityCountry;
import com.javarush.countryCache.redis.Language;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;


public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;

    private final CityDAO cityDAO;
    @SuppressWarnings("unused")
    private final CountryDAO countryDAO;

    public static void main(String[] args) {
        Main app = new Main();

        // 1) Заливаем данные из MySQL -> Redis (это НЕ часть benchmark)
        app.preloadRedisFromMysql();

        // 2) Одни и те же id (используй существующие в БД)
        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        // 3) Сравнение batch vs batch: Redis(MGET) vs MySQL(IN)
        app.benchmarkBatch(ids, 10, 200);

        app.shutdown();
    }

    public Main() {
        sessionFactory = prepareRelationalDb();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }

    public void preloadRedisFromMysql() {
        int step = 500;

        try (StatefulRedisConnection<String, String> redisConn = redisClient.connect()) {
            RedisStringCommands<String, String> redis = redisConn.sync();

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    int totalCount = cityDAO.getTotalCount(session);

                    for (int offset = 0; offset < totalCount; offset += step) {
                        List<City> batch = cityDAO.getItems(session, offset, step);
                        List<CityCountry> prepared = transformData(batch);
                        pushToRedis(prepared, redis);

                        session.clear();
                    }

                    tx.commit();
                } catch (Exception e) {
                    if (tx != null && tx.isActive()) tx.rollback();
                    throw e;
                }
            }
        }
    }

    public void benchmarkBatch(List<Integer> ids, int warmupRuns, int measuredRuns) {
        String[] redisKeys = ids.stream().map(String::valueOf).toArray(String[]::new);

        try (StatefulRedisConnection<String, String> redisConn = redisClient.connect()) {
            RedisStringCommands<String, String> redis = redisConn.sync();

            // прогрев (чтобы первый запуск не портил картину)
            for (int i = 0; i < warmupRuns; i++) {
                readFromRedisBatch(redisKeys, redis);
                readFromMysqlBatch(ids);
            }

            long redisTotal = 0;
            for (int i = 0; i < measuredRuns; i++) {
                long t0 = System.nanoTime();
                readFromRedisBatch(redisKeys, redis);
                redisTotal += (System.nanoTime() - t0);
            }

            long mysqlTotal = 0;
            for (int i = 0; i < measuredRuns; i++) {
                long t0 = System.nanoTime();
                readFromMysqlBatch(ids);
                mysqlTotal += (System.nanoTime() - t0);
            }

            System.out.printf("Redis: %.3f ms (%d runs)%n",
                    redisTotal / 1_000_000.0 / measuredRuns, measuredRuns);

            System.out.printf("MySQL: %.3f ms (%d runs)%n",
                    mysqlTotal / 1_000_000.0 / measuredRuns, measuredRuns);
        }
    }

    private List<CityCountry> readFromRedisBatch(String[] keys, RedisStringCommands<String, String> redis) {
        List<KeyValue<String, String>> values = redis.mget(keys);
        List<CityCountry> out = new ArrayList<>(values.size());

        for (KeyValue<String, String> kv : values) {
            if (!kv.hasValue()) continue;
            try {
                out.add(mapper.readValue(kv.getValue(), CityCountry.class));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse JSON from Redis for key=" + kv.getKey(), e);
            }
        }
        return out;
    }

    private List<CityCountry> readFromMysqlBatch(List<Integer> ids) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                String hql =
                        "select distinct c " +
                                "from City c " +
                                "join fetch c.country co " +
                                "join fetch co.languages " +
                                "where c.id in (:ids)";

                List<City> cities = session.createQuery(hql, City.class)
                        .setParameter("ids", ids)
                        .getResultList();


                List<CityCountry> prepared = transformData(cities);

                tx.commit();
                return prepared;
            } catch (Exception e) {
                if (tx != null && tx.isActive()) tx.rollback();
                throw e;
            }
        }
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

    private void pushToRedis(List<CityCountry> data, RedisStringCommands<String, String> redis) {
        for (CityCountry cityCountry : data) {
            try {
                redis.set(String.valueOf(cityCountry.getId()), mapper.writeValueAsString(cityCountry));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize CityCountry id=" + cityCountry.getId(), e);
            }
        }
    }

    private RedisClient prepareRedisClient() {
        RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            System.out.println("Connected to Redis");
        }
        return client;
    }

    private SessionFactory prepareRelationalDb() {
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");

        // Для более честного замера лучше обычный драйвер (p6spy добавляет оверхед).
        // properties.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
        // properties.put(Environment.URL, "jdbc:mysql://localhost:3306/world");

        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");

        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.HBM2DDL_AUTO, "validate");

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