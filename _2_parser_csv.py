import pandas as pd
import logging
import numpy as np
import os
import dotenv

dotenv.load_dotenv()

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(levellevelname)s - %(message)s'  # Ajout du nom de fichier
)
logger = logging.getLogger(__name__)

# Répertoire contenant les fichiers CSV (codé en dur)
DIRECTORY = os.getenv('DATA_DIRECTORY')

# Fonction pour trouver le fichier CSV le plus récent dans un répertoire
def get_most_recent_csv(directory):
    try:
        # Lister tous les fichiers dans le répertoire
        all_files = os.listdir(directory)
        # Filtrer les fichiers avec l'extension .csv
        csv_files = [f for f in all_files if f.endswith('output.csv')]
        if not csv_files:
            raise FileNotFoundError(f"Aucun fichier CSV trouvé dans le répertoire {directory}.")

        # Trier les fichiers par date de modification (du plus récent au plus ancien)
        csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
        # Retourner le chemin complet du fichier le plus récent
        return os.path.join(directory, csv_files[0])
    except Exception as e:
        logger.error(f"Erreur lors de la recherche du fichier CSV le plus récent : {e}")
        raise

# Obtenir le fichier CSV le plus récent
try:
    csv_file = get_most_recent_csv(DIRECTORY)
    logger.info(f"Fichier CSV le plus récent sélectionné : {csv_file}")
except FileNotFoundError as e:
    logger.error(e)
    exit(1)

# Charger le fichier CSV
logger.info("Chargement du fichier CSV...")
df = pd.read_csv(csv_file)  # Utilisation du fichier CSV le plus récent

# Vérifier si le DataFrame est vide
if df.empty:
    logger.error(f"Le fichier CSV {csv_file} est vide ou mal formaté")
    exit(1)

logger.info(f"Fichier CSV chargé avec {len(df)} lignes et {len(df.columns)} colonnes.")

# Rajouter 'TIME' comme entête de la première colonne
logger.info("Ajout de 'TIME' comme entête de la première colonne...")
df.columns.values[0] = 'TIME'
logger.info(f"Entêtes actuelles : {df.columns.tolist()}")

# Identifier les colonnes où "nuclear" apparaît en seconde ligne
logger.info("Identification des colonnes contenant 'nuclear' en seconde ligne...")
try:
    second_row = df.iloc[0]  # La seconde ligne (index 0 après l'entête)
    columns_to_keep = [col for col in df.columns if 'nuclear' in str(second_row[col]).lower()]
except IndexError:
    logger.error("Le fichier CSV ne contient pas suffisamment de lignes pour l'analyse")
    exit(1)

# Ajouter la colonne 'TIME' aux colonnes à conserver
if 'TIME' not in columns_to_keep:
    columns_to_keep.insert(0, 'TIME')
logger.info(f"Colonnes à conserver : {columns_to_keep}")

# Filtrer le DataFrame pour ne garder que les colonnes identifiées
logger.info("Filtrage des colonnes...")
filtered_df = df[columns_to_keep]
logger.info(f"DataFrame filtré avec {len(filtered_df.columns)} colonnes.")

# Trier les colonnes après la première ('TIME') par ordre alphabétique
logger.info("Tri des colonnes après la première par ordre alphabétique...")
sorted_columns = ['TIME'] + sorted(filtered_df.columns[1:])
filtered_df = filtered_df[sorted_columns]
logger.info(f"Colonnes triées : {sorted_columns}")

# Vérifier si la troisième ligne contient "Actual Consumption" et ajouter '-' devant les valeurs
logger.info("Vérification de la présence de 'Actual Consumption' dans la troisième ligne...")
third_row = filtered_df.iloc[1]  # Troisième ligne (index 1)
# Afficher les valeurs de la troisième ligne pour déboguer
logger.info(f"Valeurs de la troisième ligne :\n{third_row}")
for col in filtered_df.columns:
    # Ignorer la casse et les espaces
    logger.info(f"Test sur'{str(third_row[col]).strip().lower()}' dans la colonne '{col}'")
    if str(third_row[col]).strip().lower() == "actual consumption":
        logger.info(f"Ajout du signe '-' devant les valeurs de la colonne '{col}' à partir de la 4ème ligne.")
        # Ajouter '-' devant les valeurs à partir de la 4ème ligne (index 2)
        filtered_df.loc[2:, col] = '-' + filtered_df.loc[2:, col].astype(str)
    else:
        logger.info(f"La colonne '{col}' ne contient pas 'Actual Consumption' dans la troisième ligne.")

# Remplacer les espaces par la lettre 'T' dans la première colonne ('TIME')
logger.info("Remplacement des espaces par 'T' dans la première colonne...")
filtered_df['TIME'] = filtered_df['TIME'].str.replace(' ', 'T')
logger.info("Espaces remplacés par 'T' dans la colonne 'TIME'.")

# Supprimer la seconde et la troisième ligne (index 0 et 1 après l'entête)
logger.info("Suppression de la seconde et troisième ligne...")
filtered_df = filtered_df.drop([0, 1])  # Supprimer les lignes d'index 0 et 1
logger.info(f"DataFrame après suppression des lignes, il reste {len(filtered_df)} lignes.")

# Remplacer '-nan' par une chaîne vide dans tout le DataFrame
logger.info("Remplacement des valeurs '-nan' par une chaîne vide...")
filtered_df = filtered_df.replace('-nan', '', regex=True)
logger.info("Valeurs '-nan' remplacées par une chaîne vide.")

# Fusionner les colonnes avec des en-têtes similaires
logger.info("Fusion des colonnes avec des en-têtes similaires...")
columns_to_drop = []  # Liste pour stocker les colonnes à supprimer après fusion
for i in range(len(filtered_df.columns) - 1):
    current_col = filtered_df.columns[i]
    next_col = filtered_df.columns[i + 1]
    # Vérifier si l'en-tête de la colonne actuelle est contenu dans l'en-tête de la colonne suivante
    if current_col in next_col:
        logger.info(f"Fusion des colonnes '{current_col}' et '{next_col}'...")
        # Parcourir chaque ligne de la colonne actuelle
        for index, value in filtered_df[current_col].items():
            # Si la valeur est vide ou NaN, remplacer par la valeur correspondante de la colonne suivante
            if pd.isna(value) or value == '':
                filtered_df.at[index, current_col] = filtered_df.at[index, next_col]
        # Ajouter la colonne suivante à la liste des colonnes à supprimer
        columns_to_drop.append(next_col)

# Supprimer les colonnes fusionnées
filtered_df = filtered_df.drop(columns=columns_to_drop)
logger.info(f"Colonnes fusionnées et supprimées : {columns_to_drop}")

# Sauvegarder le résultat dans un nouveau fichier CSV dans le répertoire Grafana_Sqlite
output_filename = os.path.basename(csv_file).replace('.csv', '_filtered.csv')
output_path = os.path.join(DIRECTORY, output_filename)
logger.info(f"Sauvegarde du DataFrame filtré dans le fichier : {output_path}")
filtered_df.to_csv(output_path, index=False)
logger.info("Script terminé avec succès.")