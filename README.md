# **ETL proces datasetu NorthWind**
Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z NorthWind databázy. Projekt sa zameriava na analýzu predajov, výkonnosti zamestnancov, a nákupného správania zákazníkov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových obchodných metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom projektu je analyzovať obchodné dáta týkajúce sa produktov, objednávok a predajov. Táto analýza umožňuje:
- Identifikovať trendy v predaji,
- Hodnotiť výkonnosť zamestnancov,
- Analyzovať správanie zákazníkov.

**Tabuľky v databáze:**
- **`categories`** - Kategórie produktov
- **`products`** - Produkty a ich vlastnosti
- **`suppliers`** - Dodávatelia produktov
- **`employees`** - Zamestnanci firmy
- **`customers`** - Zákazníci
- **`orders`** - Objednávky
- **`orderdetails`** - Detaily objednávok
- **`shippers`** - Prepravcovia

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/Northwind_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma NorthWind</em>
</p>


---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)** s centrálnou faktovou tabuľkou **`fact_order_details`**, ktorá je prepojená s nasledujúcimi dimenzionálnymi tabuľkami:

### Dimenzionálne Tabuľky
- **`dim_customers`**: Údaje o zákazníkoch (SCD typ 2)
  - Atribúty: meno, kontakt, adresa
  - Uchovanie histórie zmien údajov pre historické analýzy
- **`dim_employees`**: Informácie o zamestnancoch (SCD typ 1)
  - Atribúty: meno, priezvisko, dátum narodenia
  - Údaje sa pri zmene dát prepisujú
- **`dim_products`**: Produktový katalóg (SCD typ 2)
  - Atribúty: názov, jednotka, cena, kategória
  - Uchovanie histórie zmien údajov pre historické analýzy
- **`dim_suppliers`**: Údaje o dodávateľoch (SCD typ 2)
  - Atribúty: názov, kontakt, adresa
  - Uchovanie histórie zmien údajov pre historické analýzy
- **`dim_shippers`**: Informácie o prepravcoch (SCD Typ 1)
  - Atribúty: názov, telefón
  - Údaje sa pri zmene dát prepisujú
- **`dim_order_date`**: Časová dimenzia pre dátum (SCD Typ 0)
  - Atribúty: deň, mesiac, rok, štvrťrok
  - Statické údaje
- **`dim_order_time`**: Časová dimenzia pre čas (SCD Typ 0)
  - Atribúty: hodina, AM/PM
  - Statické údaje

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/95386d5572ecd34dc51eb26d77b7fe8d1e469a6a/img/Northwind_Star_Schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre NorthWind</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `northwind_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. 

Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE northwind_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o produktoch, dodávateľoch, prepravcoch, zákazníkoch a zamestnancoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO orders_staging
FROM @northwind_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku.

---

### Dimenzia `dim_customers`

Dimenzia obsahuje údaje o zákazníkoch vrátane ich mena, kontaktu a adresy.

### Štruktúra tabuľky
| Stĺpec        | Typ          | Popis                    |
|---------------|--------------|--------------------------|
| `customer_id` | INT          | Primárny kľúč            |
| `customer_name`| VARCHAR(50) | Názov zákazníka          |
| `contact_name`| VARCHAR(50)  | Kontaktná osoba          |
| `address`     | VARCHAR(50)  | Adresa                   |
| `city`        | VARCHAR(20)  | Mesto                    |
| `postal_code` | VARCHAR(10)  | PSČ                      |
| `country`     | VARCHAR(15)  | Krajina                  |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15)
);
```
---

### Dimenzia `dim_shippers`

Tabuľka prepravcov obsahuje údaje o jednotlivých prepravcoch vrátane ich mena a kontaktu.

### Štruktúra tabuľky
| Stĺpec       | Typ         | Popis               |
|--------------|-------------|---------------------|
| `shipper_id` | INT         | Primárny kľúč       |
| `shipper_name` | VARCHAR(25) | Meno prepravcu      |
| `phone`      | VARCHAR(15) | Telefónne číslo     |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_shippers (
    shipper_id INT PRIMARY KEY,
    shipper_name VARCHAR(25),
    phone VARCHAR(15)
);
```

---

### Dimenzia `dim_employees`

Obsahuje údaje o zamestnancoch vrátane ich mena, priezviska a dátumu narodenia.

### Štruktúra tabuľky
| Stĺpec        | Typ        | Popis                 |
|---------------|------------|-----------------------|
| `employee_id` | INT        | Primárny kľúč         |
| `last_name`   | VARCHAR(15) | Priezvisko zamestnanca|
| `first_name`  | VARCHAR(15) | Meno zamestnanca      |
| `birth_date`  | DATETIME   | Dátum narodenia       |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_employees (
    employee_id INT PRIMARY KEY,
    last_name VARCHAR(15),
    first_name VARCHAR(15),
    birth_date DATETIME
);
```

---

### Dimenzia `dim_products`

Tabuľka produktov zahŕňa podrobnosti o produktoch, ako sú názvy, jednotky a cena.

### Štruktúra tabuľky
| Stĺpec        | Typ            | Popis                |
|---------------|----------------|----------------------|
| `product_id`  | INT            | Primárny kľúč        |
| `product_name`| VARCHAR(50)    | Názov produktu       |
| `unit`        | VARCHAR(25)    | Jednotka             |
| `price`       | DECIMAL(10,0)  | Cena produktu        |
| `category_name`| VARCHAR(25)   | Názov kategórie      |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50),
    unit VARCHAR(25),
    price DECIMAL(10,0),
    category_name VARCHAR(25)
);
```

---

### Dimenzia `dim_suppliers`

Dimenzia obsahuje údaje o dodávateľoch, vrátane ich mena, kontaktu a adresy.

### Štruktúra tabuľky
| Stĺpec        | Typ          | Popis                     |
|---------------|--------------|---------------------------|
| `supplier_id` | INT          | Primárny kľúč             |
| `supplier_name`| VARCHAR(50) | Názov dodávateľa          |
| `contact_name`| VARCHAR(50)  | Kontaktná osoba           |
| `address`     | VARCHAR(50)  | Adresa                    |
| `city`        | VARCHAR(20)  | Mesto                     |
| `postal_code` | VARCHAR(10)  | PSČ                       |
| `country`     | VARCHAR(15)  | Krajina                   |
| `phone`       | VARCHAR(15)  | Telefónne číslo           |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(15)
);
```

---

### Dimenzia `dim_order_date`

Obsahuje údaje o dátumoch objednávok, ktoré umožňujú analýzu podľa časových období.

### Štruktúra tabuľky
| Stĺpec              | Typ         | Popis                  |
|---------------------|-------------|------------------------|
| `order_date_id`     | INT         | Primárny kľúč          |
| `full_date`         | DATE        | Dátum objednávky       |
| `day`               | INT         | Deň objednávky         |
| `day_of_week`       | INT         | Deň v týždni (číselne) |
| `day_of_week_string`| VARCHAR(45) | Deň v týždni (text)    |
| `week`              | INT         | Týždeň v roku          |
| `month`             | INT         | Mesiac objednávky      |
| `month_string`      | VARCHAR(45) | Mesiac (text)          |
| `year`              | INT         | Rok objednávky         |
| `quarter`           | INT         | Štvrťrok               |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_order_date (
    order_date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    day_of_week INT,
    day_of_week_string VARCHAR(45),
    week INT,
    month INT,
    month_string VARCHAR(45),
    year INT,
    quarter INT
);
```

---

### Dimenzia `dim_order_time`

Tabuľka pre uchovávanie údajov o čase objednávok, zahŕňajúca hodiny, minúty a sekundy.

### Štruktúra tabuľky
| Stĺpec       | Typ         | Popis           |
|--------------|-------------|-----------------|
| `order_time_id` | INT       | Primárny kľúč   |
| `full_time`  | TIME        | Čas objednávky  |
| `hour`       | INT         | Hodina          |
| `minute`     | INT         | Minúta          |
| `second`     | INT         | Sekunda         |
| `ampm`       | VARCHAR(2)  | AM/PM indikátor |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS dim_order_time (
    order_time_id INT PRIMARY KEY,
    full_time TIME,
    hour INT,
    minute INT,
    second INT,
    ampm VARCHAR(2)
);
```

---

### Faktová tabuľka `fact_order_details`

Obsahuje prepojenie na všetky dimenzie spolu s množstvom, celkovou cenou a detailami objednávok.

### Štruktúra tabuľky
| Stĺpec            | Typ             | Popis                            |
|-------------------|-----------------|----------------------------------|
| `order_detail_id` | INT             | Primárny kľúč                   |
| `order_id`      | INT               | ID objednávky               |
| `order_date`      | DATETIME        | Dátum objednávky                |
| `quantity`        | INT             | Množstvo                        |
| `total_quantity`        | INT             | Celkové množstvo produktov v objednávke                        |
| `order_price`     | DECIMAL(10,0)   | Celková cena produktov                   |
| `order_value`     | VARCHAR(25)  | Cenové rozdelenie objednávky produktu               |
| `total_order_price`     | DECIMAL(10,0)  | Celková cena objednávky            |
| `total_order_value`     | VARCHAR(25)   | Celkové cenové rozdelenie objednávky             |
| `customer_id`     | INT             | ID zákazníka                    |
| `shipper_id`      | INT             | ID prepravcu                    |
| `employee_id`     | INT             | ID zamestnanca                  |
| `product_id`      | INT             | ID produktu                     |
| `supplier_id`     | INT             | ID dodávateľa                   |
| `order_date_id`   | INT             | ID dátumu objednávky            |
| `order_time_id`   | INT             | ID času objednávky              |

### SQL kód
```sql
CREATE TABLE IF NOT EXISTS fact_order_details (
    order_detail_id INT PRIMARY KEY,
    order_id INT,
    order_date DATETIME,
    quantity INT,
    total_quantity INT,
    items_per_order INT, 
    order_price DECIMAL(10,0),
    order_value VARCHAR(25),     
    total_order_price DECIMAL(10,0),    
    total_order_value VARCHAR(25),   
    customer_id INT,
    shipper_id INT,
    employee_id INT,
    product_id INT,
    supplier_id INT,
    order_date_id INT,
    order_time_id INT,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (shipper_id) REFERENCES dim_shippers(shipper_id),
    FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_suppliers(supplier_id),
    FOREIGN KEY (order_date_id) REFERENCES dim_order_date(order_date_id),
    FOREIGN KEY (order_time_id) REFERENCES dim_order_time(order_time_id)
);
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. 

Príklad načítania dát do dimenzií:

### Dimenzia dim_products
```sql
INSERT INTO dim_products
SELECT p.product_id, p.product_name, p.unit, p.price, c.category_name
FROM products_staging p
JOIN categories_staging c ON p.category_id = c.category_id;
```

### Dimenzia dim_order_time
```sql
INSERT INTO dim_order_time
SELECT 
    ROW_NUMBER() OVER (ORDER BY full_time) as order_time_id,
    full_time,
    hour,
    minute,
    second,
    ampm
FROM (
    SELECT DISTINCT
        TIME(order_date) as full_time,
        HOUR(order_date) as hour,
        MINUTE(order_date) as minute,
        SECOND(order_date) as second,
        CASE WHEN HOUR(order_date) < 12 THEN 'AM' ELSE 'PM' END as ampm
    FROM orders_staging
    WHERE TIME(order_date) IS NOT NULL
);
```

Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql
DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS shippers_staging;
```

ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. 
Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. 

---
## **4 Vizualizácia dát**

Dashboard obsahuje 6 vizualizácií, ktoré poskytujú komplexný prehľad o kľúčových metrikách a trendoch v oblasti predaja, zákazníkov a objednávok. Tieto vizualizácie pomáhajú lepšie pochopiť výkonnosť predaja a správanie zákazníkov.
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/dashboard.png" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard</em>
</p>

---
### Graf 1: Denné tržby

Vizualizácia zobrazuje vývoj denných tržieb v čase, umožňujúc identifikovať sezónne trendy a výkyvy v predaji. Tento pohľad je kľúčový pre pochopenie časových vzorov v predajoch a plánovanie zásob.

```sql
SELECT 
    DISTINCT f.order_id,
    d.full_date,
    f.total_order_price
FROM fact_order_details f
JOIN dim_order_date d ON f.order_date_id = d.order_date_id
ORDER BY d.full_date;
```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart1.png" alt="Dashboard">
  <br>
  <em>Obrázok 4 Graf 1</em>
</p>

---
### Graf 2: Celkové tržby podľa krajín

Graf znázorňuje distribúciu celkových tržieb medzi rôznymi krajinami. Tento pohľad pomáha identifikovať najvýnosnejšie trhy a potenciálne príležitosti pre expanziu.

```sql
SELECT 
    c.country,
    SUM(f.total_order_price) AS price_sum
FROM (
    SELECT 
        DISTINCT f.order_id, 
        f.total_order_price,
        f.customer_id
        FROM fact_order_details f
    ) AS f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY price_sum DESC;
```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart2.png" alt="Dashboard">
  <br>
  <em>Obrázok 5 Graf 2</em>
</p>

---
### Graf 3: Distribúcia hodnôt objednávok podľa krajín

Tento graf kategorizuje objednávky podľa ich hodnoty (malé, stredné, veľké, extra veľké) pre každú krajinu. Poskytuje pohľad na nákupné správanie zákazníkov v rôznych regiónoch.

```sql
SELECT 
    c.country,
    f.order_value,
    COUNT(*) as order_count
FROM fact_order_details f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.country, order_value
ORDER BY c.country, order_value;
```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart3.png" alt="Dashboard">
  <br>
  <em>Obrázok 6 Graf 3</em>
</p>

---
### Graf 4: Porovnanie predaja produktov podľa rokov

Vizualizácia zobrazuje vývoj predaja jednotlivých produktov v čase, umožňujúc sledovať trendy popularity produktov a ich životný cyklus.

```sql
SELECT 
    p.product_name, 
    d.year, 
    SUM(f.quantity) AS total_sold
FROM fact_order_details f
JOIN dim_products p ON f.product_id = p.product_id
JOIN dim_order_date d ON f.order_date_id = d.order_date_id
GROUP BY p.product_name, d.year
ORDER BY p.product_name, d.year;

```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart4.png" alt="Dashboard">
  <br>
  <em>Obrázok 7 Graf 4</em>
</p>

---
### Graf 5: Najlepšie predávané kategórie produktov

Graf znázorňuje výkonnosť rôznych produktových kategórií z hľadiska generovaných tržieb, pomáhajúc identifikovať najúspešnejšie kategórie.

```sql

SELECT 
    p.category_name,
    SUM(f.order_price) as revenue
FROM fact_order_details f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category_name
ORDER BY revenue DESC;
```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart5.png" alt="Dashboard">
  <br>
  <em>Obrázok 8 Graf 5</em>
</p>

---
### Graf 6: Výkonnosť predajcov (TOP 10)

Tento rebríček zobrazuje najúspešnejších zamestnancov podľa počtu spracovaných objednávok, poskytujúc prehľad o výkonnosti predajného tímu.
```sql
SELECT 
    CONCAT(e.first_name, ' ', e.last_name) as employee_name,
    COUNT(DISTINCT f.order_id) as total_orders,
FROM dim_employees e
JOIN fact_order_details f ON e.employee_id = f.employee_id
GROUP BY employee_name
ORDER BY total_orders DESC
LIMIT 10;


```
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/0bdbdddac273875bb0a1c08a52e823618aa9adc3/img/dashboard/chart6.png" alt="Dashboard">
  <br>
  <em>Obrázok 9 Graf 6</em>
</p>

---

Dashboard poskytuje prehľadné vizualizácie dôležitých metrík a trendov v oblasti predaja, zákazníckeho správania a objednávok. Vizualizácie umožňujú jednoducho interpretovať údaje a môžu byť využité na optimalizáciu predajnej stratégie, plánovanie zásob, expanziu na nové trhy a zlepšenie výkonnosti predajného tímu.

---
**Autor:** Patrik Kavan
