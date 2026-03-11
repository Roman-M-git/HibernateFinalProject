package com.javarush.countryCache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(1)
public class BenchmarkTest {

    private SessionFactory sessionFactory;
    private CityDAO cityDAO;

    private RedisClient redisClient;
    private ObjectMapper objectMapper;

    // используй существующие id из world.city
    private final List<Integer> testIds = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

    public static void main(String[] args) {
        Options options = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .build();
        try {
            new Runner(options).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        sessionFactory = prepareRelationalDb();
        cityDAO = new CityDAO(sessionFactory);

        redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        objectMapper = new ObjectMapper();

        // Заполняем Redis данными из MySQL
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();

            int totalCount = cityDAO.getTotalCount(session);
            int step = 500;

            try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
                RedisStringCommands<String, String> sync = connection.sync();

                for (int offset = 0; offset < totalCount; offset += step) {
                    List<City> batch = cityDAO.getItems(session, offset, step);


                    List<CityCountry> prepared = batch.stream().map(city -> {
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

                    for (CityCountry cc : prepared) {
                        try {
                            sync.set("city:" + cc.getId(), objectMapper.writeValueAsString(cc));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    session.clear();
                }
            }

            session.getTransaction().commit();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (sessionFactory != null) sessionFactory.close();
        if (redisClient != null) redisClient.shutdown();
    }

    @Benchmark
    public void readFromRedis() {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (Integer id : testIds) {
                String json = sync.get("city:" + id);
                objectMapper.readValue(json, CityCountry.class);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void readFromMySQL() {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();

            for (Integer id : testIds) {
                City city = session.createQuery(
                                "select distinct c " +
                                        "from City c " +
                                        "join fetch c.country co " +
                                        "join fetch co.languages " +
                                        "where c.id = :id",
                                City.class
                        ).setParameter("id", id)
                        .getResultList()
                        .get(0);

                city.getCountry().getLanguages().size();
            }

            session.getTransaction().commit();
        }
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