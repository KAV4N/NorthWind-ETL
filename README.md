# **ETL proces datasetu NorthWind**
Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z NorthWind databázy. Projekt sa zameriava na analýzu predajov, výkonnosti dodávateľov a zamestnancov, a nákupného správania zákazníkov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových obchodných metrik.

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
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/f50737c412df7f36985c21f38612ff9fe00d3c51/northwind_erd/Northwind_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma NorthWind</em>
</p>


---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)** s centrálnou faktovou tabuľkou **`fact_order_details`**, ktorá je prepojená s nasledujúcimi dimenzionálnymi tabuľkami:

### Dimenzionálne Tabuľky
- **`dim_customers`**: Údaje o zákazníkoch
  - Atribúty: meno, kontakt, adresa
- **`dim_employees`**: Informácie o zamestnancoch
  - Atribúty: meno, priezvisko, dátum narodenia
- **`dim_products`**: Produktový katalóg
  - Atribúty: názov, jednotka, cena, kategória
- **`dim_suppliers`**: Údaje o dodávateľoch
  - Atribúty: názov, kontakt, adresa
- **`dim_shippers`**: Informácie o prepravcoch
  - Atribúty: názov, telefón
- **`dim_order_date`**: Časová dimenzia pre dátum
  - Atribúty: deň, mesiac, rok, štvrťrok
- **`dim_order_time`**: Časová dimenzia pre čas
  - Atribúty: hodina, AM/PM

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.
<p align="center">
  <img src="https://github.com/KAV4N/NorthWind-ETL/blob/f50737c412df7f36985c21f38612ff9fe00d3c51/northwind_star_schema/StarSchema_NorthWind.png" alt="ERD Schema">
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
COPY INTO occupations_staging
FROM @northwind_stage/Products_staging.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

---
### **3.1 Transform (Transformácia dát)**

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
| `order_date`      | DATETIME        | Dátum objednávky                |
| `quantity`        | INT             | Množstvo                        |
| `total_price`     | DECIMAL(10,0)   | Celková cena                    |
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
    order_date DATETIME,
    quantity INT,
    total_price DECIMAL(10,0),
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

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

---
### **Graf 1: Najviac hodnotené knihy (Top 10 kníh)**

---
### **Graf 2: Rozdelenie hodnotení podľa pohlavia používateľov**

---
### **Graf 3: Trendy hodnotení kníh podľa rokov vydania (2000–2024)**

---
### **Graf 4: Celková aktivita počas dní v týždni**

---
### **Graf 5: Počet hodnotení podľa povolaní**

---
### **Graf 6: Aktivita používateľov počas dňa podľa vekových kategórií**

---

**Autor:** Patrik Kavan
