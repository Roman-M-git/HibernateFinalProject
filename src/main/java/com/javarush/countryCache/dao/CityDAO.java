package com.javarush.countryCache.dao;

import com.javarush.countryCache.domain.City;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class CityDAO {

    private final SessionFactory sessionFactory;

    public CityDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<City> getItems(Session session, int offset, int limit) {
        Query<City> query = session.createQuery("select c from City c", City.class);
        query.setFirstResult(offset);
        query.setMaxResults(limit);
        return query.getResultList(); // или query.list()
    }

    public int getTotalCount(Session session) {
        Query<Long> query = session.createQuery("select count(c) from City c", Long.class);
        Long count = query.getSingleResult(); // или uniqueResult()
        return Math.toIntExact(count);
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}