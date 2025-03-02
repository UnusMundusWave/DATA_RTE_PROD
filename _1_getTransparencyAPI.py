import os
import dotenv
from entsoe import EntsoePandasClient
import pytz
import pandas as pd
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

dotenv.load_dotenv()
token = os.getenv('API_TOKEN')
DATA_DIRECTORY = os.getenv('DATA_DIRECTORY')

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
client = EntsoePandasClient(api_key=token, timeout=60)

# Définir la période de données que vous souhaitez récupérer avec des objets datetime conscients du fuseau horaire
cet = pytz.timezone('Europe/Paris')
start = pd.Timestamp(datetime.now() - timedelta(hours=80), tz='Europe/Paris')
end = pd.Timestamp(datetime.now()- timedelta(hours=0), tz='Europe/Paris')
# start = pd.Timestamp(datetime.now() - timedelta(hours=240), tz='Europe/Paris')
# end = pd.Timestamp(datetime.now()- timedelta(hours=0), tz='Europe/Paris')

# Créer un nom de fichier avec horodatage et nom du script
script_name = os.path.basename(__file__)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = DATA_DIRECTORY  # Nom du dossier
# Vérifier si le dossier existe, sinon le créer
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
output_filename = f"{output_folder}\\{timestamp}_{script_name.replace('.py', '')}_output.csv"

@retry_strategy
def query_data(country_code, start, end):
    """
    Récupère les données de production par unité pour un pays donné, avec une stratégie de relance.
    """
    return client.query_generation_per_plant(country_code=country_code, start=start, end=end)

try:
    # Exemple de requête pour obtenir les données de production par unité en France
    df = query_data(country_code='FR', start=start, end=end)
    
    # Sauvegarder les données dans un fichier CSV
    df.to_csv(output_filename)
    print(f"[{script_name}] Les données ont été sauvegardées dans {output_filename}")

except Exception as e:
    print(f"[{script_name}] Une erreur s'est produite : {str(e)}")

# Note: Assurez-vous que l'erreur n'est pas liée à la manipulation des dates dans le DataFrame 
# Si vous avez besoin de convertir des fuseaux horaires dans un DataFrame, utilisez :
# df['time_column'] = df['time_column'].dt.tz_convert('Nouveau_Fuseau_Hor')