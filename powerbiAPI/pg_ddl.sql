--tables in postgresql 

pbi_metadata=# \d pbi_dashboard_tiles
                          Table "public.pbi_dashboard_tiles"
   Column    |            Type             | Collation | Nullable |      Default
-------------+-----------------------------+-----------+----------+-------------------
 id          | character varying(100)      |           |          |
 title       | character varying(100)      |           |          |
 reportid    | character varying(100)      |           |          |
 datasetid   | character varying(100)      |           |          |
 dashboardid | character varying(100)      |           |          |
 workspaceid | character varying(100)      |           |          |
 create_date | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_dashboards
                            Table "public.pbi_dashboards"
   Column    |            Type             | Collation | Nullable |      Default
-------------+-----------------------------+-----------+----------+-------------------
 id          | character varying(100)      |           |          |
 displayname | character varying(100)      |           |          |
 workspaceid | character varying(100)      |           |          |
 create_date | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_datasets
                                  Table "public.pbi_datasets"
        Column        |            Type             | Collation | Nullable |      Default
----------------------+-----------------------------+-----------+----------+-------------------
 id                   | character varying(100)      |           |          |
 name                 | character varying(100)      |           |          |
 configuredby         | character varying(100)      |           |          |
 datasourceinstanceid | character varying(100)      |           |          |
 workspaceid          | character varying(100)      |           |          |
 create_date          | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_datasources
                             Table "public.pbi_datasources"
     Column     |            Type             | Collation | Nullable |      Default
----------------+-----------------------------+-----------+----------+-------------------
 datasourceid   | character varying(100)      |           |          |
 datasourcetype | character varying(100)      |           |          |
 server         | character varying(100)      |           |          |
 database       | character varying(100)      |           |          |
 workspaceid    | character varying(100)      |           |          |
 create_date    | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_dsmodel_tables
                          Table "public.pbi_dsmodel_tables"
   Column    |            Type             | Collation | Nullable |      Default
-------------+-----------------------------+-----------+----------+-------------------
 id          | integer                     |           |          |
 name        | character varying(100)      |           |          |
 description | character varying(200)      |           |          |
 datasetid   | character varying(100)      |           |          |
 workspaceid | character varying(100)      |           |          |
 create_date | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_reports
                                Table "public.pbi_reports"
      Column      |            Type             | Collation | Nullable |      Default
------------------+-----------------------------+-----------+----------+-------------------
 id               | character varying(100)      |           |          |
 name             | character varying(100)      |           |          |
 reporttype       | character varying(100)      |           |          |
 createddatetime  | character varying(50)       |           |          |
 modifieddatetime | character varying(50)       |           |          |
 modifiedby       | character varying(100)      |           |          |
 datasetid        | character varying(100)      |           |          |
 workspaceid      | character varying(100)      |           |          |
 endorsement      | character varying(100)      |           |          |
 certifiedby      | character varying(100)      |           |          |
 create_date      | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_workspaces
                            Table "public.pbi_workspaces"
   Column    |            Type             | Collation | Nullable |      Default
-------------+-----------------------------+-----------+----------+-------------------
 id          | character varying(100)      |           |          |
 name        | character varying(100)      |           |          |
 type        | character varying(100)      |           |          |
 state       | character varying(100)      |           |          |
 create_date | timestamp without time zone |           |          | CURRENT_TIMESTAMP

pbi_metadata=# \d pbi_event_log
                                             Table "public.pbi_event_log"
      Column       |           Type           | Collation | Nullable |                     Default
-------------------+--------------------------+-----------+----------+-------------------------------------------------
 event_id          | integer                  |           | not null | nextval('pbi_event_log_event_id_seq'::regclass)
 event_timestamp   | timestamp with time zone |           |          | CURRENT_TIMESTAMP
 event_type        | character varying(100)   |           |          |
 event_category    | character varying(100)   |           |          |
 event_subcategory | character varying(100)   |           |          |
 event_name        | character varying(1000)  |           |          |
 event_log         | text                     |           |          |
Indexes:
    "pbi_event_log_pkey" PRIMARY KEY, btree (event_id)

