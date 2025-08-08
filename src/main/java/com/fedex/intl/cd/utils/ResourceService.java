package com.fedex.intl.cd.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

@Slf4j
@Service()
public class ResourceService {

  @Autowired
  private ResourceLoader resourceLoader;

  public Resource getResource (String path) {
    Resource resource = null;
    try {
      resource = resourceLoader.getResource(path);
    } catch (Exception e) {
      log.error("getResource(), Error getting resource: {}", path);
    }
    return resource;
  }

}
