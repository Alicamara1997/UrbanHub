# UrbanHub – Plateforme Smart City (projet complet)

Jumeau numérique urbain pour **Paris, Lyon, Marseille, Toulouse et Bordeaux** : collecte de trois flux de données (Batch, Streaming, IoT), stockage dans un data lake MinIO en architecture médaillon (Bronze → Silver → Gold), traitement Spark, analyses et graphiques, puis croisement des sources.

> Projet réalisé dans le cadre de la formation IPSSI (MIA5). Ce dépôt contient **les 4 parties**, réalisées par moi de bout en bout.

## Architecture

```
NOAA + Open-Meteo        CityBikes API            OpenAQ API
 (météo, batch)       (vélos, toutes 3 min)   (pollution, toutes 10 min)
       │                     │                        │
       └──────────── Python : collecte ───────────────┘
                             ▼
            MinIO  🥉 Bronze : données brutes
                             │  Spark (JupyterLab)
                             ▼
                   🥈 Silver : données nettoyées
                             │  Spark
                             ▼
                   🥇 Gold : indicateurs métier
                             │  pandas + matplotlib
                             ▼
              📊 Analyses, graphiques, croisement (Partie 4)
```

**Technologies :** Docker Compose · MinIO (S3) · Python (requests, pandas) · Apache Spark 3.5 (PySpark) · JupyterLab · Parquet · matplotlib

## Contenu du dépôt

| Dossier / fichier | Rôle |
|---|---|
| `docker-compose.yml` | MinIO, JupyterLab/PySpark, et les 2 collectes en continu (vélos, pollution) qui redémarrent seules |
| `scripts/meteo_batch.py` | Partie 1 – téléchargement **en parallèle** des stations météo françaises NOAA (5 ans) |
| `scripts/meteo_recente.py` | Météo récente Open-Meteo (92 jours, heure par heure) pour la Partie 4 |
| `scripts/velos_stream.py` | Partie 2 – collecte des stations de vélos (CityBikes) toutes les 3 minutes |
| `scripts/pollution_iot.py` | Partie 3 – capteurs de pollution (OpenAQ) : référentiel, historique 90 jours, flux IoT toutes les 10 minutes |
| `scripts/export_gold_meteo.py` | Export des tables Gold météo en CSV |
| `notebooks/meteo_silver_gold.ipynb` | Partie 1 – nettoyage, Gold, 4 questions |
| `notebooks/velos_silver_gold.ipynb` | Partie 2 – nettoyage, mouvements, 5 questions |
| `notebooks/pollution_silver_gold.ipynb` | Partie 3 – nettoyage, Gold, 4 questions |
| `notebooks/partie4_croisement.ipynb` | Partie 4 – jointure météo × pollution × vélos, 4 questions |
| `figures/` | Tous les graphiques produits |

## Lancer le projet

```bash
cp .env.example .env          # puis mettre sa clé OpenAQ (gratuite)
docker compose up -d          # MinIO + Jupyter + collectes vélos et pollution
```
- JupyterLab : http://localhost:8888 · Console MinIO : http://localhost:9001
- Météo : `docker exec urbanhub-jupyter python /home/jovyan/work/scripts/meteo_batch.py` puis `meteo_recente.py`
- Pollution (une fois) : `pollution_iot.py capteurs` puis `pollution_iot.py historique 90`
- Ouvrir les notebooks dans l'ordre : météo → vélos → pollution → partie 4 (**un seul notebook Spark ouvert à la fois**)

---

## Partie 1 – Flux Batch : météo (NOAA)

- **Collecte :** 211 stations françaises, 2021 → août 2025, téléchargées en parallèle (16 à la fois) : 14 millions de lignes, 0 erreur
- **Nettoyage :** décodage des codes NOAA, valeurs manquantes et douteuses écartées, doublons fusionnés → **11 millions de lignes**, stations rattachées aux villes (< 30 km)

| Question | Résultat |
|---|---|
| Périodes anormales | 28 jours anormalement chauds, 24 froids ; ex. vague de froid de février 2021, canicule de fin juin 2025 |
| Météo et visibilité | Corrélations faibles ; la pluie et le brouillard (hiver) réduisent la visibilité |
| Saisons | France : 5,6 °C en janvier → 21,5 °C en août ; Marseille la plus chaude, Paris l'été le plus frais |
| Jours extrêmes | Toulouse la plus exposée à la chaleur, Marseille au vent (mistral) ; records : 43 °C, tempête Ciarán |

## Partie 2 – Flux Streaming : vélos (CityBikes)

- **Collecte :** environ 2 900 stations des 5 villes toutes les 3 minutes, dans MinIO (bronze/velos)
- **Nettoyage :** format `renting` différent selon les villes (`1` à Paris, `true` ailleurs) harmonisé, stations fermées écartées, heure de Paris ajoutée, taux de remplissage
- **Méthode :** l'API donne le nombre de vélos, pas les trajets → on compare chaque relevé au précédent (`lag`) : baisse = départs, hausse = arrivées

| Question | Résultat (premiers jours de collecte) |
|---|---|
| Q1 Stations les plus utilisées | Stations proches des **gares, métros et trams** (Part-Dieu à Lyon, Cartoucherie à Toulouse) |
| Q2 Zones à l'offre insuffisante | Lyon 28 % des zones, Paris 19 %, Toulouse 14 %, Bordeaux 8 % |
| Q3 Pics journaliers | 9 h à Paris (trajet domicile → travail), 17 h à Lyon, Toulouse, Bordeaux |
| Q4 Déséquilibres géographiques | Paris : centre plein (62 %) et proche banlieue vide (30 %) |
| Q5 Stations critiques | Surtout des stations **vides** à réapprovisionner (Paris 145, Lyon 135) |

**Limite :** l'API ne met pas à jour le réseau de **Marseille** (aucun mouvement, la moitié des stations « fermées ») → Marseille est exclue des analyses vélos.

## Partie 3 – Flux IoT : pollution (OpenAQ)

- **Collecte :** 191 capteurs actifs (PM2.5, PM10, NO2, O3, CO) à moins de 25 km des 5 villes ; historique 90 jours (295 671 mesures horaires) + flux toutes les 10 minutes

| Question | Résultat |
|---|---|
| Zones les plus polluées | Abords des **autoroutes et périphériques** (A1 à Saint-Denis : NO2 = 2 × le seuil OMS) |
| Variation journalière | Pics de NO2 vers **9 h et 22 h**, creux l'après-midi |
| Polluant dominant (indice = moyenne / seuil OMS) | **NO2** à Paris, Lyon, Marseille (0,99 : presque au seuil) ; **ozone** à Toulouse et Bordeaux |
| Épisodes anormaux | NO2 fréquent à Marseille (40 jours sur 90) ; **incendie de Gironde** : PM2.5 à 6 × le seuil à Bordeaux le 26 juillet 2026 |

## Partie 4 – Analyse croisée

Jointure sur **ville + heure** : 10 710 heures avec météo et pollution, et les heures où les vélos sont disponibles.

| Question | Résultat |
|---|---|
| Météo ↔ pollution | **Chaleur → ozone** (corrélation +0,72) ; **vent → NO2** (−0,35 : le vent disperse la pollution) ; humidité → moins d'ozone |
| Météo ↔ vélos | Premiers résultats : +8 % d'utilisation par météo favorable (à confirmer avec plus de jours) |
| Météo favorable au vélo | 12 à 28 °C, sans pluie, vent faible |
| Interactions fortes | Jours **« air bloqué »** (pas de vent + NO2 élevé) : Paris 16, Marseille 14, Lyon 10 ; épisodes de particules fines (dont l'incendie de Bordeaux) |

## Limites

- NOAA s'arrête au 24 août 2025 → la météo récente vient d'Open-Meteo (modèle, pas de stations)
- Vélos : l'historique commence au lancement de la collecte (pas d'historique CityBikes) ; résultats à confirmer sur plusieurs semaines
- Les mouvements de vélos sont une estimation basse (un départ et une arrivée entre deux relevés s'annulent)
