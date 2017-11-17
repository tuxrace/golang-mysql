SELECT
  trafficId,
  funnelId,
  visitorId,
  trafficSourceId,
  trafficTimestamp,
  isNewVisitor,
  actionNum,
  actionTypeId,
  actionTimestamp,
  isNewClick,
  hitId,
  nodeId,
  nodeTypeId,
  hitTimestamp,
  cost,
  CONCAT("{", "'kw':'",SUBSTRING(deviceType,0,5), "'}") as trackingFields,
  CONCAT("{",
    "'type':'",deviceType, "',",
    "'brand':'", deviceBrand, "',",
    "'model':'", deviceModel, "',",
    "'name':'", deviceName ,"',",
    "'manufacturer':'", deviceManufacturer ,"',",
    "'os':'", coalesce(deviceOs,'smart'),"',",
    "'browser':'", deviceBrowser,"',",
    "'osVersion':'", coalesce(deviceOsVersion,'smart'),"',",
    "'browserVersion':'", deviceBrowserVersion ,"',",
    "'displayWidth':'", deviceDisplayWidth ,"',",
    "'displayResolution':'", deviceDisplayResolution ,"',",
    "'displayHeight':'", deviceDisplayHeight ,"',",
    "'browserOtherLanguages':'", deviceBrowserOtherLanguages ,"',",
    "'browserMainLanguage':'", deviceBrowserMainLanguage ,"'",
  "}") as device,
  CONCAT("{",
    "'country':'",locationCountry, "',",
    "'city':'", locationCity, "',",
    "'region':'", locationRegion,"'",  
  "}") as location,
  CONCAT("{",
    "'referrerDomain':'",coalesce(connectionReferrerDomain,'smart'), "',",
    "'referrer':'", coalesce(connectionReferrer,'smart'), "',",
    "'ispAsn':'", coalesce(connectionIspAsn,'smart'), "',",  
    "'isp':", coalesce(connectionIsp,'smart'), "',",
    "'ip':", coalesce(connectionIp,'smart'),"'",   
  "}") as connection

FROM

  [project-flux-testing:flux.traffic_data]