import os
import dotenv
from entsoe import EntsoePandasClient
import pytz
import pandas as pd
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
import logging
import glob # Added for finding files

# --- Configuration ---
dotenv.load_dotenv()
token = os.getenv('API_TOKEN')
DATA_DIRECTORY = os.getenv('DATA_DIRECTORY')

# --- Mode Configuration ---
HISTORY_MODE = True # Set to True to enable history filling mode
GAP_THRESHOLD_HOURS = 3 # Minimum gap duration (in hours) to trigger history fill
MAX_HISTORY_FETCH_HOURS = 240 # Maximum duration (in hours) to fetch in one history run
# --- End Mode Configuration ---

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check if DATA_DIRECTORY is set
if not DATA_DIRECTORY:
    logger.error("La variable d'environnement DATA_DIRECTORY n'est pas définie.")
    exit() # Exit if the directory isn't specified

# --- ENTSO-e Client and Retry Strategy ---
# Définir le nombre maximal de tentatives et le délai d'attente initial pour la stratégie de relance
MAX_RETRIES = 5
INITIAL_WAIT = 1 # secondes

# Définir une fonction pour vérifier si l'exception est une erreur de connexion
def is_connection_error(exception):
    return isinstance(exception, requests.exceptions.ConnectionError) or \
           isinstance(exception, requests.exceptions.Timeout) or \
           isinstance(exception, requests.exceptions.ConnectTimeout)

# Définir une stratégie de relance avec un délai exponentiel et un arrêt après un certain nombre de tentatives
retry_strategy = retry(
    retry=retry_if_exception_type(is_connection_error),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=INITIAL_WAIT, min=INITIAL_WAIT, max=60), # Délai entre 1 et 60 secondes
    reraise=True  # Important: relève l'exception après le nombre maximal de tentatives
)

# Initialiser le client avec un timeout plus long (en secondes)
client = EntsoePandasClient(api_key=token, timeout=120) # Increased timeout further
logger.info("Client EntsoePandasClient initialisé.")

# --- File Handling ---
script_name = os.path.basename(__file__) if '__file__' in locals() else 'interactive_script' # Handle interactive use
timestamp_now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = DATA_DIRECTORY  # Nom du dossier

# Vérifier si le dossier existe, sinon le créer
if not os.path.exists(output_folder):
    try:
        os.makedirs(output_folder)
        logger.info(f"Le dossier {output_folder} a été créé.")
    except OSError as e:
        logger.error(f"Erreur lors de la création du dossier {output_folder}: {e}")
        exit() # Exit if directory creation fails

output_filename_template = f"{output_folder}/{timestamp_now_str}_{script_name.replace('.py', '')}_output.csv"

# --- Timezone ---
cet = pytz.timezone('Europe/Paris')

# --- Helper Functions ---

def get_start_time_normal_mode(output_folder):
    """
    (Normal Mode) Détermine l'heure de début en fonction du fichier le plus récent
    dans le dossier de sortie. Si aucun fichier n'est trouvé ou si une erreur
    survient, retourne maintenant - 2 heures.
    """
    try:
        # Trouver le fichier le plus récent basé sur le nom (qui contient le timestamp)
        list_of_files = glob.glob(os.path.join(output_folder, '*_output.csv'))
        if not list_of_files:
            logger.info("Aucun fichier CSV trouvé pour le mode normal. Utilisation de now - 2 heures.")
            return pd.Timestamp(datetime.now(cet) - timedelta(hours=2)).tz_convert(cet)

        latest_file = max(list_of_files, key=os.path.getctime) # Find by creation/modification time
        logger.info(f"Mode normal: Fichier le plus récent trouvé : {os.path.basename(latest_file)}")

        # Lire la dernière date du fichier le plus récent
        # Important: Assumer que la première colonne est l'index de temps
        df_latest = pd.read_csv(latest_file, index_col=0, parse_dates=True)
        if df_latest.empty:
             logger.warning(f"Le fichier {latest_file} est vide. Utilisation de now - 2 heures.")
             return pd.Timestamp(datetime.now(cet) - timedelta(hours=2)).tz_convert(cet)

        # Assurer que l'index est de type DatetimeIndex et localisé
        if not isinstance(df_latest.index, pd.DatetimeIndex):
             raise ValueError("L'index du fichier CSV n'est pas un DatetimeIndex.")
        if df_latest.index.tz is None:
             df_latest.index = df_latest.index.tz_localize(cet, ambiguous='infer') # Localize if naive
        else:
             df_latest.index = df_latest.index.tz_convert(cet) # Convert if different tz

        last_timestamp_in_file = df_latest.index.max()
        logger.info(f"Mode normal: Dernier timestamp trouvé dans {os.path.basename(latest_file)}: {last_timestamp_in_file}")

        # Le nouveau start est juste après le dernier timestamp trouvé
        # Ajouter une petite marge (ex: 1 minute) pour éviter de récupérer la même donnée exacte
        new_start = last_timestamp_in_file + timedelta(minutes=1)
        logger.info(f"Mode normal: Heure de début calculée : {new_start}")
        return new_start.tz_convert(cet) # Ensure timezone

    except FileNotFoundError:
        logger.info("Aucun fichier CSV trouvé pour le mode normal (FileNotFound). Utilisation de now - 2 heures.")
        return pd.Timestamp(datetime.now(cet) - timedelta(hours=2)).tz_convert(cet)
    except pd.errors.EmptyDataError:
         logger.warning(f"Le fichier {latest_file} semble vide (EmptyDataError). Utilisation de now - 2 heures.")
         return pd.Timestamp(datetime.now(cet) - timedelta(hours=2)).tz_convert(cet)
    except Exception as e:
        logger.error(f"Erreur lors de la détermination de l'heure de début en mode normal : {e}. Utilisation de now - 2 heures.")
        return pd.Timestamp(datetime.now(cet) - timedelta(hours=2)).tz_convert(cet)


def load_all_timestamps(output_folder, tz):
    """
    (History Mode) Charge tous les timestamps uniques de tous les fichiers CSV
    dans le dossier de sortie et les retourne triés.
    """
    all_timestamps = set()
    csv_files = glob.glob(os.path.join(output_folder, '*_output.csv'))
    logger.info(f"Mode historique: Recherche des fichiers CSV dans {output_folder}. Trouvé {len(csv_files)} fichiers.")

    for f in csv_files:
        try:
            # Lire seulement l'index (colonne 0) pour l'efficacité
            df = pd.read_csv(f, index_col=0, parse_dates=True, usecols=[0])
            if not df.empty:
                 # Assurer la localisation/conversion du fuseau horaire
                 if df.index.tz is None:
                     df.index = df.index.tz_localize(tz, ambiguous='infer')
                 else:
                     df.index = df.index.tz_convert(tz)
                 all_timestamps.update(df.index.tolist())
            else:
                 logger.warning(f"Mode historique: Fichier vide ignoré: {os.path.basename(f)}")

        except pd.errors.EmptyDataError:
             logger.warning(f"Mode historique: Fichier vide (EmptyDataError) ignoré: {os.path.basename(f)}")
        except ValueError as ve:
            logger.warning(f"Mode historique: Problème de parsing de date ou d'index dans {os.path.basename(f)}: {ve}. Fichier ignoré.")
        except Exception as e:
            logger.error(f"Mode historique: Erreur de lecture du fichier {os.path.basename(f)}: {e}. Fichier ignoré.")

    if not all_timestamps:
        logger.info("Mode historique: Aucun timestamp n'a pu être chargé depuis les fichiers CSV.")
        return pd.Series([], dtype='datetime64[ns, Europe/Paris]') # Return empty Series with correct dtype and tz

    # Convertir en Series pandas, trier et supprimer les doublons (au cas où)
    sorted_timestamps = pd.Series(list(all_timestamps), dtype=f'datetime64[ns, {tz.zone}]').sort_values().unique()
    logger.info(f"Mode historique: {len(sorted_timestamps)} timestamps uniques chargés et triés.")
    return pd.Series(sorted_timestamps) # Return as Series for easier diff calculation

def find_first_gap(sorted_timestamps, gap_threshold_td):
    """
    (History Mode) Trouve le premier écart entre timestamps consécutifs
    qui est plus grand que le seuil défini.
    Retourne (start_time_of_gap, end_time_of_gap) ou (None, None).
    """
    if len(sorted_timestamps) < 2:
        logger.info("Mode historique: Pas assez de timestamps pour trouver un écart.")
        return None, None # Cannot find a gap with less than 2 points

    # Calculer la différence entre timestamps consécutifs
    diffs = sorted_timestamps.diff().iloc[1:] # iloc[1:] to skip the first NaN difference

    # Trouver le premier index où la différence dépasse le seuil
    gap_indices = diffs[diffs > gap_threshold_td].index
    # logger.debug(f"Différences calculées:\n{diffs}") # DEBUG
    # logger.debug(f"Indices des écarts trouvés: {gap_indices}") # DEBUG

    if not gap_indices.empty:
        first_gap_index = gap_indices[0]
        # Le gap est ENTRE l'index précédent et l'index actuel
        gap_start_time = sorted_timestamps.iloc[first_gap_index - 1]
        gap_end_time = sorted_timestamps.iloc[first_gap_index]
        gap_duration = gap_end_time - gap_start_time
        logger.info(f"Mode historique: Écart trouvé > {gap_threshold_td}. Début: {gap_start_time}, Fin: {gap_end_time}, Durée: {gap_duration}")
        return gap_start_time, gap_end_time
    else:
        logger.info(f"Mode historique: Aucun écart supérieur à {gap_threshold_td} trouvé dans les données existantes.")
        return None, None

@retry_strategy
def query_data(country_code, start, end):
    """
    Récupère les données de production par unité pour un pays donné,
    avec une stratégie de relance.
    """
    # Ensure start/end have the correct timezone right before query
    start_aware = start.tz_convert('Europe/Paris')
    end_aware = end.tz_convert('Europe/Paris')

    # Log the final adjusted query times
    logger.info(f"Requête ENTSO-e pour {country_code} de {start_aware} à {end_aware}...")

    # Add a check: if start >= end, don't query
    if start_aware >= end_aware:
        logger.warning(f"Heure de début ({start_aware}) est après ou égale à l'heure de fin ({end_aware}). Aucune requête effectuée.")
        # Return an empty DataFrame matching the expected structure if possible
        # This structure might vary, so returning an empty generic DataFrame
        return pd.DataFrame()

    try:
        data = client.query_generation_per_plant(country_code=country_code, start=start_aware, end=end_aware)
        logger.info(f"Requête réussie. {len(data) if data is not None else 0} lignes reçues.")
        # Ensure the resulting DataFrame index is timezone-aware
        if data is not None and not data.empty:
             if data.index.tz is None:
                 data.index = data.index.tz_localize('Europe/Paris', ambiguous='infer')
             else:
                 data.index = data.index.tz_convert('Europe/Paris')
        return data
    except Exception as e:
        logger.error(f"Erreur pendant la requête ENTSO-e après tentatives: {e}", exc_info=True) # Log traceback
        # Re-raise the exception if retry_strategy didn't handle it or reraise=True
        raise


# --- Main Logic ---
start_fetch = None
end_fetch = None
df_result = None

if HISTORY_MODE:
    logger.info("--- Mode Historique activé ---")
    gap_threshold_td = timedelta(hours=GAP_THRESHOLD_HOURS)
    max_fetch_td = timedelta(hours=MAX_HISTORY_FETCH_HOURS)

    # 1. Charger tous les timestamps existants
    all_timestamps = load_all_timestamps(output_folder, cet)

    if all_timestamps.empty:
        # Cas où il n'y a AUCUNE donnée historique
        logger.info("Mode historique: Aucune donnée existante trouvée. Tentative de récupération des dernières {} heures.".format(MAX_HISTORY_FETCH_HOURS))
        end_fetch = pd.Timestamp(datetime.now(cet))
        start_fetch = end_fetch - max_fetch_td
        logger.info(f"Mode historique: Période initiale de récupération : Début={start_fetch}, Fin={end_fetch}")
    else:
        # 2. Trouver le premier écart significatif
        gap_start, gap_end = find_first_gap(all_timestamps, gap_threshold_td)

        if gap_start is not None and gap_end is not None:
            # 3. Définir la période de récupération pour combler l'écart
            start_fetch = gap_start + timedelta(seconds=1) # Start just after the last known data point before the gap
            potential_end_fetch = gap_end - timedelta(seconds=1) # End just before the first known data point after the gap

            # Limiter la durée de récupération à MAX_HISTORY_FETCH_HOURS
            if (potential_end_fetch - start_fetch) > max_fetch_td:
                end_fetch = start_fetch + max_fetch_td
                logger.info(f"Mode historique: Durée de l'écart ({potential_end_fetch - start_fetch}) dépasse le maximum ({max_fetch_td}). Limite de la fin à {end_fetch}.")
            else:
                end_fetch = potential_end_fetch

            logger.info(f"Mode historique: Période de récupération de l'écart : Début={start_fetch}, Fin={end_fetch}")
        else:
            logger.info("Mode historique: Aucun écart nécessitant un comblement n'a été trouvé. Le script va se terminer.")
            # start_fetch et end_fetch restent None, donc pas de requête

else: # Normal Mode (History = False)
    logger.info("--- Mode Normal activé ---")
    start_fetch = get_start_time_normal_mode(output_folder)
    end_fetch = pd.Timestamp(datetime.now(cet)).tz_convert(cet) # Ensure timezone
    logger.info(f"Mode normal: Période de récupération : Début={start_fetch}, Fin={end_fetch}")

# --- Exécution de la requête et sauvegarde (si une période a été définie) ---
if start_fetch is not None and end_fetch is not None and start_fetch < end_fetch:
    try:
        # Exemple de requête pour obtenir les données de production par unité en France
        df_result = query_data(country_code='FR', start=start_fetch, end=end_fetch)

        if df_result is not None and not df_result.empty:
            # Construire le nom de fichier final avec les dates réelles utilisées
            start_str = start_fetch.strftime('%Y%m%d%H%M')
            end_str = end_fetch.strftime('%Y%m%d%H%M')
            mode_str = "HIST" if HISTORY_MODE else "NORM"
            final_output_filename = f"{output_folder}/{script_name.replace('.py', '')}_{mode_str}_{start_str}_to_{end_str}.csv"

            # Sauvegarder les données dans un fichier CSV
            df_result.to_csv(final_output_filename)
            logger.info(f"Les données ({len(df_result)} lignes) ont été sauvegardées dans {final_output_filename}")
        elif df_result is not None and df_result.empty:
             logger.info("La requête n'a retourné aucune donnée pour la période spécifiée.")
        else:
             logger.warning("La requête a retourné None.")

    except Exception as e:
        # Log l'erreur finale si la requête échoue après les relances
        logger.error(f"Échec final de la récupération des données après plusieurs tentatives : {e}", exc_info=True) # Log traceback

elif start_fetch is not None and end_fetch is not None and start_fetch >= end_fetch:
    logger.warning(f"Calcul de période invalide (début >= fin): Début={start_fetch}, Fin={end_fetch}. Aucune donnée récupérée.")
else:
    logger.info("Aucune période de récupération valide définie. Aucune requête effectuée.")


logger.info("--- Fin du script ---")

# En mode historique, on sort explicitement après une tentative (qu'elle ait réussi, échoué ou trouvé aucun gap)
if HISTORY_MODE:
    logger.info("Mode historique: Sortie du script après une tentative de comblement.")
    # L'exécution se termine ici naturellement