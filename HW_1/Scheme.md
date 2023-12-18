```mermaid
classDiagram
class Website {
  - data
}  
class CRM {
  - data
}
class SocialNetworks {
  - data  
}
<<external>> ExternalSources

class ETL {
  + extract()
  + transform()  
  + load()
}

<<datastore>> DataWarehouse {
  - marketing_data
}

class BI {
  + analyze()  
  + visualize()
  + reports()  
}

class Marketers
class TopManagement 
class OtherStakeholders

 Website -- ETL
 CRM -- ETL
 SocialNetworks -- ETL
 ExternalSources -- ETL
 
 ETL -- DataWarehouse
 
 DataWarehouse -- BI
 
 BI -- Marketers
 BI -- TopManagement
 BI -- OtherStakeholders
 ```