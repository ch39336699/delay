package com.fedex.intl.cd.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor

public class Request {
  private String rulesSetName;
  private String messagePayLoad;
}
