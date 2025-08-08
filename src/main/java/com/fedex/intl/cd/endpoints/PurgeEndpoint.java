package com.fedex.intl.cd.endpoints;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

@Endpoint(id = "purge")
public class PurgeEndpoint {

  @Value("${spring.application.name}")
  String appName;

  @Value("${vcap.application.instance_id: 1234}")
  public String instanceID;

  @ReadOperation
  public String invoke() {
    return appName + "+" + instanceID;
  }
}

