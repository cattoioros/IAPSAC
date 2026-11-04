# AutoScout24 Used Car Listings Dataset

## 1. De ce acest dataset?

Scopul proiectului este construirea unui dataset cu anunțuri auto second-hand din Europa, colectate de pe AutoScout24, pentru analiza pieței și dezvoltarea unor modele de AI.

Datasetul ar putea fi utilizat pentru:

- **Predicția prețurilor:** estimarea valorii unei mașini pe baza caracteristicilor sale.
- **Analiza pieței:** studierea relației dintre preț, vechime, kilometraj, marcă și motorizare.
- **Analiza evoluției prețurilor:** identificarea modificărilor de preț pentru anunțurile observate în mai multe momente.
- **Detectarea anunțurilor atipice:** identificarea mașinilor cu prețuri neobișnuit de mici sau de mari în raport cu vehicule similare.

Spre deosebire de dataseturile istorice disponibile public, proiectul urmărește colectarea unor date recente. În plus, datele vor fi normalizate într-o schemă comună, validate și pregătite pentru utilizare în experimente de machine learning.

Un dataset existent ar putea fi suficient pentru construirea unui model inițial, însă nu garantează accesul la anunțuri actuale sau posibilitatea de a urmări evoluția prețurilor în timp. Datasetul propus urmărește să acopere aceste nevoi.

## 2. Surse

**Sursa principală:** AutoScout24.

- Site: [https://www.autoscout24.ro/](https://www.autoscout24.ro/)
- Pagina de anunțuri: [https://www.autoscout24.ro/lst](https://www.autoscout24.ro/lst)
- Surse secundare (deduplicare): [https://www.autovit.ro/autoturisme](https://www.autovit.ro/autoturisme) [https://www.mobile.de/](https://www.mobile.de/)

**Licență și drepturi de utilizare:** Codul proiectului va avea o licență open-source separată de drepturile asupra datelor. Datele colectate nu vor fi publicate automat sub aceeași licență cu codul.&#x20;

**Dimensiune estimată:** 5.000–10.000 de înregistrări inițiale, cu posibilitatea extinderii în funcție de disponibilitatea datelor și de limitele permisiunii primite.

## 3. Date sensibile și confidențialitate

Anunțurile pot conține date cu caracter personal, în special atunci când sunt publicate de persoane fizice. De exemplu, numele vânzătorilor, numerele de telefon și alte date de contact nu sunt necesare pentru obiectivele proiectului.

Pentru a reduce riscurile privind confidențialitatea:

- Nu vom colecta intenționat nume de persoane, numere de telefon sau adrese de e-mail.
- Nu vom include date de contact în datasetul final.
- Vom păstra numai atributele necesare analizei vehiculelor.
- Datele brute nu vor fi încărcate pe GitHub.

Vom evalua și conținutul textual al anunțurilor înainte de a-l păstra, deoarece descrierile pot include informații personale.

## 4. Schema datasetului

Fiecare înregistrare va reprezenta un anunț auto și va conține atributele relevante pentru analiza prețului.

| Câmp             | Tip          | Descriere                                 |
| ---------------- | ------------ | ----------------------------------------- |
| listing_id       | string       | Identificator intern al înregistrării     |
| make             | string       | Marca vehiculului                         |
| model            | string       | Modelul vehiculului                       |
| year             | integer      | Anul primei înmatriculări                 |
| mileage_km       | integer      | Kilometrajul în kilometri                 |
| fuel_type        | string       | Tipul de combustibil                      |
| transmission     | string       | Tipul transmisiei                         |
| power_hp         | integer/null | Puterea in HP                             |
| price_euro       | number       | Prețul cerut                              |
| country          | string       | Țara în care este publicat anunțul        |
| first_seen_at    | datetime     | Momentul primei observări de către sistem |
| last_seen_at     | datetime     | Momentul ultimei observări                |
| source           | string       | Sursa anunțului                           |
| premium_features | json         | Lista de dotari premium                   |
| basic_features   | json         | Lista de dotari standard                  |



Câmpurile opționale vor putea avea valoarea `null` dacă informația nu este disponibilă. Prețurile vor fi păstrate cu moneda originală, iar conversiile valutare, dacă vor fi necesare, vor fi documentate separat.
