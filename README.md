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

Navrhnutý bol **hviezdicový model (star schema)** s centrálnou faktovou tabuľkou **`fact_orderdetails`**, ktorá je prepojená s nasledujúcimi dimenzionálnymi tabuľkami:

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
### **3.1 Transfor (Transformácia dát)**

---
### **3.3 Load (Načítanie dát)**


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
