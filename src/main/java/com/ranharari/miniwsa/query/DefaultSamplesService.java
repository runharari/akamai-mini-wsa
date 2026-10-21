package com.ranharari.miniwsa.query;


import java.util.List;

public class DefaultSamplesService implements SamplesService {

    private final SamplesRepository repository;

    public DefaultSamplesService(SamplesRepository repository) {
        this.repository = repository;
    }

    @Override
    public SamplesPage samples(SampleFilter filter, int limit, int offset) {
        long total = repository.count(filter);
        List<EventSample> events = repository.find(filter, limit, offset);
        return new SamplesPage(total, limit, offset, events);
    }
}
