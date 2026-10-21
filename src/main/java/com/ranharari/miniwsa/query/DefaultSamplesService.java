package com.ranharari.miniwsa.query;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Profile({"query", "default"})
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
