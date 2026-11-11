# Data proposal: anunțuri auto second-hand din România și 8 țări UE

Eșantion din datele reale: [`docs/sample/sample_40.json`](sample/sample_40.json) (și
[`.csv`](sample/sample_40.csv)): 24 de anunțuri AutoScout24 (3 din fiecare țară) și 16 Autovit.

## 1. De ce acest dataset?

Vrem să estimăm **prețul corect al unei mașini second-hand** și să comparăm piața din România cu
piețele din care se importă cele mai multe mașini (Germania, Italia, Olanda, Belgia, Franța, Austria,
Spania, Luxemburg).

**Cine l-ar folosi:** cumpărători care vor să știe dacă un preț e bun sau dacă merită importul;
dealeri care evaluează un stoc; studenți și cercetători care antrenează modele de predicție a
prețului sau de detectare a anunțurilor cu preț atipic.

**Ce aduce în plus:** seturile publice de anunțuri auto (de ex. cele de pe Kaggle) sunt în general
vechi de câțiva ani, dintr-o singură țară, fără lista de dotări și fără România. Setul nostru este:

- **recent** (anunțuri active în 4–10 noiembrie 2026);
- **multi-țară**, cu aceeași schemă pentru 9 țări, deci se poate compara aceeași mașină între țări;
- **mare** pentru AutoScout24 (peste 1 milion de anunțuri), ca modelele să aibă date și pentru
  mărci/modele rare;
- **curățat și documentat**: categorii unificate între site-uri, valori imposibile marcate
  (`dq_flags`), duplicate eliminate, motivul pentru care un rând nu intră în setul de antrenare.

## 2. Surse

| Sursă | Link | Cum colectăm | Dimensiune |
|---|---|---|---|
| Autovit.ro (România) | https://www.autovit.ro/autoturisme | Playwright, pagina fiecărui anunț (`get_links.py`, `car_details.py`) | 7.732 anunțuri (15 MB JSON), 83% cu dotări |
| AutoScout24 (DE, IT, NL, BE, FR, AT, ES, LU) | https://www.autoscout24.ro/lst | Varianta JSON a paginilor de căutare și de anunț (`autoscout24_scraper/`) | ~2,19 mil. anunțuri pe site; 1.372.517 colectate, din care **1.028.777 cu 19/20 câmpuri** (862 MB JSON) |

**Licență.** Datele nu au licență deschisă: conținutul aparține platformelor și vânzătorilor, iar
termenii de utilizare ai ambelor site-uri restricționează extragerea automată și reutilizarea. De
aceea: folosim datele doar în scop educațional, necomercial; colectăm lent (~5 cereri/s), fără
proxy-uri și fără a ocoli limitările site-ului; **nu publicăm setul complet**, ci doar codul și
eșantionul de 40 de rânduri. Codul din repo are o licență separată de date.

## 3. Date sensibile

Paginile de anunț conțin date personale ale vânzătorilor persoane fizice: nume, telefon, WhatsApp,
adresă și coordonate, plus descrierea liberă (în care apar uneori nume sau telefoane).

- **Nu le colectăm.** Scraperul AutoScout24 le șterge înainte de a scrie ceva pe disc, inclusiv din
  datele brute păstrate local (`sanitize_details` în `autoscout24_scraper/carscraper/mapper.py`,
  acoperit de teste). Scraperul Autovit citește doar secțiunile despre mașină (detalii, preț, dotări).
- **Setul final are doar atribute ale vehiculului.** Nu păstrăm descrieri, VIN sau poze.
- **Risc rămas:** `listing_id` și `url` duc la anunțul public, unde vânzătorul își afișează singur
  datele. `url` nu intră în setul curățat, iar anunțurile expiră în câteva săptămâni.
- **Datele brute nu sunt pe GitHub** (`data/`, `export/` și `*.json` sunt în `.gitignore`); în repo
  intră doar eșantionul.

## 4. Schema

O înregistrare = un anunț. Ambele scrapere scriu același format (cel din `cars.json` de la Autovit);
`scripts/curate_cars.py` și `scripts/curate_autoscout.py` îl curăță, iar `scripts/merge_curated.py`
unește cele două surse.

| Câmp | Tip | Descriere | Completat: AutoScout24 / Autovit |
|---|---|---|---|
| `listing_id` | string | ID-ul anunțului pe site | 100% / 100% |
| `make`, `model` | string | Marcă, model | ~100%, 97,9% / 98,7% |
| `version` | string \| null | Versiunea (text liber) | 99,7% / 65,4% |
| `year` | int | Anul (AS24: înmatriculării; Autovit: fabricației) | 95,4% / 98,7% |
| `generation` | string \| null | Generația modelului | 0% (nu există pe AS24) / 51,1% |
| `first_registration_date` | string | Prima înmatriculare (`august 2017`) | 95,4% / 77,1% |
| `mileage_km` | int | Kilometraj | 99,1% / 98,6% |
| `fuel_type`, `transmission`, `body_type` | string | Combustibil, cutie, caroserie | 99,7–100% / 98,7% |
| `engine_displacement_cm3` | int \| null | Capacitate cilindrică (null la electrice) | 86,7% / 98,7% |
| `power_hp` | int | Putere (CP) | 98,9% / 97,8% |
| `price_euro` | int | Preț cerut, EUR | 100% / 98,7% |
| `color`, `doors` | string, int | Culoare, număr de uși | 99,9–100% / 98,4–98,7% |
| `country` | string | Țara anunțului (`Germania`, `Romania`, ...) | 100% / 100% |
| `first_seen_at` | datetime | Când l-am văzut prima dată | 100% / 100% |
| `source` | string | `AutoScout24` sau `Autovit.ro` | 100% / 100% |
| `features` | list[string] \| null | Dotări | 0,33% în setul 19/20 (doar 3.364 anunțuri) / 83,2% |
| `url` | string | Doar AutoScout24 | 100% / – |

Setul curățat păstrează aceste câmpuri cu categorii în engleză (`diesel`, `automatic`, `suv`) și
adaugă coloane derivate: `model_family`, `age_years`, `km_per_year`, `is_new`, `log_price`,
`dq_flags` (ce s-a corectat), `train_exclude_reasons`.

**Exemple** (dotările scurtate):

```json
{"listing_id": "c51feaf4-884c-494c-90a7-1c7fb91ad6cc", "make": "Volkswagen", "model": "ID.7",
 "version": "Pro VOLL/MATRIX/PANORAMA/NAVI-PRO/HEAD-UP/EXTER.+", "year": 2023, "generation": null,
 "first_registration_date": "noiembrie 2023", "mileage_km": 34280, "fuel_type": "Electric",
 "transmission": "Automata", "body_type": "Berlina", "engine_displacement_cm3": null, "power_hp": 286,
 "price_euro": 46789, "color": "Negru", "doors": 4, "country": "Germania",
 "first_seen_at": "2026-11-09 15:02:03.536314", "source": "AutoScout24",
 "features": ["360° camera", "Acoperis panoramic", "Aer conditionat automat, 3 zone", "... (64)"],
 "url": "https://www.autoscout24.ro/oferte/volkswagen-id-7-pro-...-c51feaf4-884c-494c-90a7-1c7fb91ad6cc"}

{"listing_id": "1f7e5675-7b3c-46ab-8c91-6ca421b963c7", "make": "Volkswagen", "model": "Amarok",
 "version": "3.0 TDI Aventura 10GA 4M Navi Leder AHK", "year": 2026, "generation": null,
 "first_registration_date": "august 2026", "mileage_km": 3000, "fuel_type": "Motorina",
 "transmission": "Automata", "body_type": "Off-road/4x4/Pick-up", "engine_displacement_cm3": 2989,
 "power_hp": 241, "price_euro": 69990, "color": "Verde", "doors": 5, "country": "Germania",
 "first_seen_at": "2026-11-09 19:23:36.692383", "source": "AutoScout24", "features": null,
 "url": "https://www.autoscout24.ro/oferte/volkswagen-amarok-...-1f7e5675-7b3c-46ab-8c91-6ca421b963c7"}

{"listing_id": "ID7HR05J", "make": "Mercedes-Benz", "model": "C", "version": "250 d Aut.",
 "year": 2013, "generation": null, "first_registration_date": "3 mai 2013", "mileage_km": 173380,
 "fuel_type": "Diesel", "transmission": "Automata", "body_type": "Sedan",
 "engine_displacement_cm3": 2143, "power_hp": 204, "price_euro": 8900, "color": "Alb", "doors": 4,
 "country": "Romania", "first_seen_at": "2026-11-04 20:38:53.416127", "source": "Autovit.ro",
 "features": ["Bluetooth", "Radio", "Sistem hands-free", "... (36)"]}
```

Eșantionul arată și de ce e nevoie de curățare: un Audi A4 din Austria are prețul de 999 €
(valoare-placeholder), iar etichetele diferă între site-uri (`Motorina` / `Diesel`, `Berlina` / `Sedan`).

## 5. Cum arătăm că setul e util

Antrenăm același model de preț (de ex. gradient boosting pe `log_price`) în două variante: doar pe
Autovit și pe Autovit + AutoScout24 (cu `country` și `source` ca intrări). Comparăm eroarea (MAE,
MAPE) pe un set de test **doar din anunțuri Autovit**. Dacă datele din UE scad eroarea, mai ales la
mărcile și modelele rare în România, setul își face treaba. Ca bonus, aceeași mașină (marcă, model,
an, km apropiați) comparată între țări arată unde e mai ieftină și cât de mare e diferența față de
România.
