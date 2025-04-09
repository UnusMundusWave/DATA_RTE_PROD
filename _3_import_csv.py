import sqlite3
import csv
from dateutil import parser  # Utilisé pour analyser les dates ISO 8601
import os
import dotenv

dotenv.load_dotenv()

# Configuration
db_path = 'production.db'  # Chemin vers votre base de données SQLite
DIRECTORY = os.getenv('DATA_DIRECTORY')  # Répertoire contenant les fichiers CSV

# Fonction pour trouver le fichier CSV le plus récent avec le suffixe _filtered.csv
def get_most_recent_filtered_csv(directory):
    try:
        # Lister tous les fichiers dans le répertoire
        all_files = os.listdir(directory)
        # Filtrer les fichiers avec l'extension .csv et le suffixe _filtered
        filtered_files = [f for f in all_files if f.endswith('_filtered.csv')]
        if not filtered_files:
            raise FileNotFoundError(f"Aucun fichier CSV avec le suffixe '_filtered.csv' trouvé dans {directory}.")
        # Trier les fichiers par date de modification (du plus récent au plus ancien)
        filtered_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
        # Retourner le chemin complet du fichier le plus récent
        return os.path.join(directory, filtered_files[0])
    except Exception as e:
        print(f"Erreur lors de la recherche du fichier CSV le plus récent : {e}")
        raise

# Connexion à la base de données
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Vérifier si la table "units" a une contrainte UNIQUE sur la colonne "name"
try:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS units (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,  -- Ajout de la contrainte UNIQUE ici
        location TEXT,
        production_type TEXT,
        installation_date TEXT,
        characteristics TEXT
    )
    ''')
except sqlite3.OperationalError as e:
    print(f"Erreur lors de la création/modification de la table units : {e}")

# Créer la table `production` si elle n'existe pas
try:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS production (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unit_id INTEGER,
        timestamp TEXT,
        value REAL,
        UNIQUE(unit_id, timestamp)  -- Contrainte pour éviter les doublons
    )
    ''')
except sqlite3.OperationalError as e:
    print(f"Erreur lors de la création/modification de la table production : {e}")

# Fonction pour formater les dates au format ISO 8601 (remplacer espace par 'T')
def format_date(date_str):
    try:
        # Analyser la date avec dateutil.parser
        dt = parser.parse(date_str)  # Ici, pas de conflit car "parser" est bien celui de dateutil
        # Formater au format ISO 8601 sans fuseau horaire
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        raise ValueError(f"Format de date invalide : {date_str}")

# Obtenir le fichier CSV le plus récent avec le suffixe _filtered.csv
try:
    csv_file = get_most_recent_filtered_csv(DIRECTORY)
    print(f"[{os.path.basename(__file__)}] Fichier CSV sélectionné : {csv_file}")
except FileNotFoundError as e:
    print(e)
    exit(1)

# Étape 1 : Lire le fichier CSV
with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)  # Lit le fichier CSV avec les en-têtes comme clés
    headers = reader.fieldnames  # Récupère les en-têtes (première ligne)
    # Les en-têtes contiennent "TIME" et les noms des unités
    if 'TIME' not in headers:
        raise ValueError("Le fichier CSV doit contenir une colonne 'TIME'.")
    
    unit_names = headers[1:]  # Les noms des unités sont les autres colonnes
    # Dictionnaire pour stocker les IDs des unités
    units = {}
    # Étape 2 : Insérer ou récupérer les unités dans la table `units`
    for unit_name in unit_names:
        cursor.execute('SELECT id FROM units WHERE name = ?', (unit_name,))
        unit = cursor.fetchone()
        if unit is None:
            # L'unité n'existe pas, on l'insère
            cursor.execute('''
            INSERT INTO units (name, location, production_type, installation_date, characteristics)
            VALUES (?, ?, ?, ?, ?)
            ''', (unit_name, 'Unknown', 'Unknown', '2023-01-01', '{}'))
            unit_id = cursor.lastrowid  # Récupérer l'id de l'unité insérée
            print(f"[{os.path.basename(__file__)}] [DEBUG] Unité '{unit_name}' insérée avec succès. Nouvel ID : {unit_id}")
        else:
            # L'unité existe déjà, on récupère son id
            unit_id = unit[0]
            print(f"[{os.path.basename(__file__)}] [DEBUG] Unité '{unit_name}' déjà existante. ID récupéré : {unit_id}")
        
        # Stocker l'id de l'unité pour utilisation ultérieure
        units[unit_name] = unit_id

    # Étape 3 : Insérer les données de production dans la table `production`
    for row in reader:
        timestamp = format_date(row['TIME'])  # Formatter la date
        for unit_name in unit_names:
            value = row[unit_name]
            if value:  # Ignorer les valeurs vides
                try:
                    value = float(value)  # Convertir la valeur en nombre
                except ValueError:
                    print(f"[{os.path.basename(__file__)}] Valeur invalide ignorée : {value} pour {unit_name} à {timestamp}")
                    continue
                # Vérifier si le timestamp existe déjà pour cette unité
                cursor.execute('''
                SELECT id FROM production WHERE unit_id = ? AND timestamp = ?
                ''', (units[unit_name], timestamp))
                existing_record = cursor.fetchone()
                if existing_record:
                    print(f"[{os.path.basename(__file__)}] [DEBUG] Données déjà existantes pour {unit_name} à {timestamp}, ignorées.")
                else:
                    # Insérer la donnée de production
                    try:
                        cursor.execute('''
                        INSERT INTO production (unit_id, timestamp, value)
                        VALUES (?, ?, ?)
                        ''', (units[unit_name], timestamp, value))
                        #print(f"[DEBUG] Données insérées pour {unit_name} à {timestamp} avec la valeur {value}.")
                    except sqlite3.IntegrityError as e:
                        print(f"[{os.path.basename(__file__)}] [DEBUG] Erreur lors de l'insertion des données pour {unit_name} à {timestamp}: {e}")

# Validation et fermeture
conn.commit()
conn.close()
print(f"[{os.path.basename(__file__)}] Importation terminée avec succès.")