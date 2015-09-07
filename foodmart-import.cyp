CREATE CONSTRAINT ON (r:Region) ASSERT r.id IS UNIQUE;
CREATE CONSTRAINT ON (r:Region) ASSERT r.name IS UNIQUE;
CREATE CONSTRAINT ON (c:Customer) ASSERT c.id IS UNIQUE;
CREATE CONSTRAINT ON (c:Customer) ASSERT c.account_num IS UNIQUE;
CREATE CONSTRAINT ON (pf:ProductFamily) ASSERT pf.name IS UNIQUE;
CREATE CONSTRAINT ON (d:Department) ASSERT d.name IS UNIQUE;
CREATE CONSTRAINT ON (pc:ProductCategory) ASSERT pc.name IS UNIQUE;
CREATE CONSTRAINT ON (ps:ProductSubCategory) ASSERT ps.id IS UNIQUE;
CREATE CONSTRAINT ON (b:Brand) ASSERT b.name IS UNIQUE;
CREATE CONSTRAINT ON (p:Product) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (st:StoreType) ASSERT st.name IS UNIQUE;
CREATE CONSTRAINT ON (s:Store) ASSERT s.id IS UNIQUE;
CREATE CONSTRAINT ON (y:Year) ASSERT y.year IS UNIQUE;
CREATE CONSTRAINT ON (m:Month) ASSERT m.id IS UNIQUE;
CREATE CONSTRAINT ON (d:Date) ASSERT d.id IS UNIQUE;
CREATE CONSTRAINT ON (d:Date) ASSERT d.date IS UNIQUE;
CREATE CONSTRAINT ON (d:Date) ASSERT d.day IS UNIQUE;
CREATE CONSTRAINT ON (p:Promotion) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (s:Sale) ASSERT s.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/region.csv" AS line
MERGE (r:Region {id: line.region_id})
ON CREATE
SET r.sales_city = line.sales_city
, r.sales_state_province = line.sales_state_province
, r.sales_district = line.sales_district
, r.sales_region = line.sales_region
, r.sales_country = line.sales_country
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/customer.csv" AS line
OPTIONAL MATCH (r:Region {id: line.customer_region_id})
MERGE (c:Customer {id: line.customer_id})
ON CREATE
SET c.account_num = line.account_num
, c.lname = line.lname
, c.fname = line.fname
, c.mi = line.mi
, c.address1 = line.address1
, c.address2 = line.address2
, c.address3 = line.address3
, c.address4 = line.address4
, c.city = line.city
, c.state_province = line.state_province
, c.postal_code = line.postal_code
, c.country = line.country
, c.phone1 = line.phone1
, c.phone2 = line.phone2
, c.birthdate = line.birthdate
, c.marital_status = line.marital_status
, c.yearly_income = line.yearly_income
, c.gender = line.gender
, c.total_children = line.total_children
, c.num_children_at_home = line.num_children_at_home
, c.education = line.education
, c.date_accnt_opened = line.date_accnt_opened
, c.member_card = line.member_card
, c.occupation = line.occupation
, c.houseowner = CASE WHEN line.houseowner = "Y" THEN true ELSE false END
, c.num_cars_owned = line.num_cars_owned
, c.fullname = line.fullname
MERGE (c)-[:IN_REGION]->(r)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/product_class.csv" AS line
MERGE (pf:ProductFamily {name: line.family})
MERGE (pc:ProductCategory {name: line.category})
MERGE (d:Department {name: line.department})
MERGE (ps:ProductSubCategory {id: line.product_class_id})
ON CREATE
SET ps.name = line.subcategory
MERGE (ps)-[:IN_FAMILY]->(pf)
MERGE (ps)-[:IN_CATEGORY]->(pc)
MERGE (ps)-[:IN_DEPARTMENT]->(d)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/product.csv" AS line
OPTIONAL MATCH (ps:ProductSubCategory {id: line.product_class_id})
MERGE (b:Brand {name: line.brand_name})
MERGE (p:Product {id: line.product_id})
ON CREATE
SET p.name = line.product_name
, p.SKU = line.SKU
, p.SRP = line.SRP
, p.gross_weight = line.gross_weight
, p.net_weight = line.net_weight
, p.recyclable_package = line.recyclable_package
, p.low_fat = line.low_fat
, p.units_per_case = line.units_per_case
, p.cases_per_pallet = line.cases_per_pallet
, p.shelf_width = line.shelf_width
, p.shelf_height = line.shelf_height
, p.shelf_depth = line.shelf_depth
MERGE (p)-[:IN_CATEGORY]->(ps)
MERGE (p)-[:FROM_BRAND]->(b)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/store.csv" AS line
OPTIONAL MATCH (r:Region {id: line.region_id})
MERGE (c:City {name: line.store_city, state: line.store_state, country: line.store_country})
MERGE (st:StoreType {name: line.store_type})
MERGE (s:Store {id: line.store_id})
ON CREATE
SET s.name = line.store_name
, s.number = line.store_number
, s.street_address = line.store_street_address
, s.city = line.store_city
, s.state = line.store_state
, s.postal_code = line.store_postal_code
, s.country = line.store_country
, s.manager = line.store_manager
, s.phone = line.store_phone
, s.fax = line.store_fax
, s.first_opened_date = line.first_opened_date
, s.last_remodel_date = line.last_remodel_date
, s.sqft = line.store_sqft
, s.grocery_sqft = line.grocery_sqft
, s.frozen_sqft = line.frozen_sqft
, s.meat_sqft = line.meat_sqft
, s.coffee_bar = toInt(line.coffee_bar) > 0
, s.video_store = toInt(line.video_store) > 0
, s.salad_bar = toInt(line.salad_bar) > 0
, s.prepared_food = toInt(line.prepared_food) > 0
, s.florist = toInt(line.florist) > 0
MERGE (s)-[:OF_TYPE]->(st)
MERGE (s)-[:IN_CITY]->(c)
MERGE (c)-[:IN_REGION]->(r)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/time_by_day.csv" AS line
MERGE (y:Year {year: toInt(line.the_year)})
MERGE (m:Month {id: line.the_year + "-" + line.the_month})
ON CREATE
SET m.month = toInt(line.the_month)
MERGE (m)<-[:HAS_YEAR]-(y)
MERGE (d:Date {id: line.time_id})
ON CREATE
SET d.date = line.the_date
, d.day = toInt(line.day_since_epoch)
, d.day_of_month = toInt(line.day_of_month)
, d.day_of_week = line.the_day
MERGE (d)<-[:HAS_DAY]-(m)
;

MATCH (d:Date)
MATCH (next:Date {day: d.day+1})
MERGE (d)-[:NEXT_DAY]->(next)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/promotion.csv" AS line
OPTIONAL MATCH (sd:Date {date: line.start_date})
OPTIONAL MATCH (ed:Date {date: line.end_date})
MERGE (p:Promotion {id: line.promotion_id})
ON CREATE
SET p.name = line.promotion_name
, p.media_type = line.media_type
, p.cost = toInt(line.cost)
, p.start_date = line.start_date
, p.end_date = line.end_date
MERGE (p)-[:STARTS_ON]->(sd)
MERGE (p)-[:ENDS_ON]->(ed)
;

LOAD CSV WITH HEADERS FROM "https://github.com/neo4j-examples/neo4j-foodmart-dataset/raw/master/data/sales.csv" AS line
OPTIONAL MATCH (d:Date {id: line.time_id})
OPTIONAL MATCH (st:Store {id: line.store_id})
OPTIONAL MATCH (c:Customer {id: line.customer_id})
MERGE (s:Sale {id: line.time_id + "-" + line.customer_id})
MERGE (s)-[:ON_DATE]->(d)
MERGE (s)-[:IN_STORE]->(st)
MERGE (s)-[:PURCHASED_BY]->(c)
MERGE (p:Product {id: line.product_id})
MERGE (s)-[i:LINE_ITEM]->(p)
ON CREATE
SET i.price = toInt(replace(line.store_sales, ".", ""))
, i.cost_price = toInt(replace(line.store_cost, ".", ""))
, i.quantity = toInt(replace(line.unit_sales, ".", ""))
WITH s, line
FOREACH (id IN (CASE WHEN line.promotion_id IS NULL THEN [] ELSE [line.promotion_id] END) |
  MERGE (pr:Promotion {id: id})
  ON CREATE SET pr.fail = (pr.id / 0) // abort if it didn't match!
  MERGE (s)-[:APPLIED_PROMOTION]->(pr)
)
;
