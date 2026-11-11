# carscraper (AutoScout24)

Scraper pentru AutoScout24 (toate cele 8 țări: DE, IT, NL, BE, FR, AT, ES, LU). Exportă anunțurile în
același format ca `cars.json` de pe Autovit (aceleași 20 de câmpuri, în aceeași ordine, plus `url`).
Exportul (`autoscout24_19din20.json`) este intrarea pentru `scripts/curate_autoscout.py` din rădăcina
repo-ului.

Rezultatul primei rulări (9–10 noiembrie 2026): 1.372.517 anunțuri unice, dintre care 1.028.777 cu
19 din 20 de câmpuri (tot în afară de `generation`). Datele nu sunt în git (`data/`, `export/`).

## Cum merge

- Citește varianta JSON a paginilor (`/_next/data/...`), nu HTML; fără browser.
- AutoScout24 blochează un IP ~2 minute după ~1.000 de cereri rapide, așa că scraperul merge la o
  viteză constantă (~5 cereri/s, se ajustează singură) și face pauză dacă primește 429/403.
  Nu folosește proxy-uri și nu ocolește limitele site-ului.
- **Modul `auto` (implicit)** are trei etape:
  1. **atribute**: parcurge paginile de căutare filtrate după caroserie, culoare și număr de uși, așa
     că aceste 3 câmpuri vin din filtru. 20 de anunțuri pe cerere, cu 19 din 20 de câmpuri
     (~75 de anunțuri/s).
  2. **catalog**: căutările fără acele filtre, pentru anunțurile care nu au caroserie/culoare/uși
     completate pe site (12 câmpuri din pagina de căutare).
  3. **detalii**: deschide pagina fiecărui anunț și completează restul (dotări, versiune completă).
     O cerere per anunț, ~400k/zi.
- Țările se iau pe rând în toate etapele, deci oriunde te oprești ai un amestec din toate.
- Căutările cu peste 4.000 de rezultate (limita site-ului) se împart automat: marcă → interval de
  preț → an (în etapa de atribute: caroserie → culoare → uși → preț → marcă → an).
- Totul e în `data/autoscout24.db` (SQLite). Oprit și pornit din nou, continuă de unde a rămas.
- Datele vânzătorului (telefon, nume, adresă, coordonate, descriere) nu se salvează nici în datele
  brute (`carscraper/mapper.py: sanitize_details`).

## Comenzi

```bash
cd autoscout24_scraper
uv sync
uv run carscraper supervise -- --mode auto     # rulare nesupravegheată, cu repornire automată
uv run carscraper run                          # o singură rulare (Ctrl+C = oprire curată)
uv run carscraper stats                        # cât s-a strâns, pe țări și niveluri
uv run carscraper export --min-19 -o export/autoscout24_19din20.json   # setul 19/20
uv run carscraper export --only-full           # doar anunțurile cu toate cele 20 de câmpuri
uv run carscraper export --format jsonl -o data/autoscout24.jsonl
uv run python scripts/export_sqlite.py         # export/autoscout24_19din20.db (tabelul `cars`)
uv run --with matplotlib python scripts/stats_card_19.py   # card PNG cu statistici
```

`run` acceptă: `--mode auto|attrs|catalog|details|pipeline`, `--countries DE,IT`, `--max-listings N`,
`--rate 5` (cereri/s la pornire; implicit ultima viteză care a mers), `--max-rate 8`.

Pe macOS, `run` și `supervise` țin Mac-ul treaz cât rulează (`caffeinate -ims`); închiderea capacului
tot îl adoarme, iar pe baterie se oprește când se descarcă.

## Supraveghere

`supervise` repornește scraperul dacă:
- se termină cu eroare (după 60 s; renunță după 5 erori într-o oră și scrie `OPRIT` în log),
- site-ul îl blochează mai mult de ~35 de minute (așteaptă 15 min și reia),
- nu mai salvează nimic 45 de minute (îl oprește și îl repornește).

Loguri: `data/run.log` (progres la fiecare 15 s) și `data/supervisor.log` (porniri, erori, reporniri).

## Niveluri ale unui anunț în baza de date

| `level` | Ce are |
|---|---|
| `basic` | 12 câmpuri din pagina de căutare (fără caroserie, culoare, uși, dotări) |
| `attrs` | 19/20: plus caroserie, culoare, uși (din filtrele căutării); `features` = `null` |
| `full` | 20/20 cu dotări, din pagina anunțului |
| `gone` | Anunțul a dispărut înainte să-i fie citită pagina |

## Câmpuri diferite față de Autovit

- `generation`: AutoScout24 dă doar un număr intern, deci e mereu `null`.
- `first_registration_date`: doar luna și anul (`"august 2017"`); `year` = anul înmatriculării.
- Valorile sunt etichetele în română ale AutoScout24, fără diacritice (`Manuala`, `Motorina`,
  `Berlina`), nu identice cu ale Autovit (`Diesel`, `Sedan`). Le unifică `scripts/curate_autoscout.py`.
- `features`: doar pentru anunțurile `full`; în setul 19/20 e `null`.

## Teste

```bash
uv run pytest
```
