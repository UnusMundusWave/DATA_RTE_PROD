import os
import sqlite3
import asyncio
from telegram import Bot
from dotenv import load_dotenv  
import json
import logging
from pathlib import Path
from datetime import datetime

load_dotenv()

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')  # Token du bot Telegram
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')               # ID du chat (utilisateur ou groupe)
DB_PATH = 'production.db'                             # Chemin vers la base de données


# Configuration du logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    f'%(asctime)s - {Path(__file__).name} - %(levelname)s - %(message)s'
))
logger.addHandler(handler)

# Initialisation du bot Telegram
bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def send_telegram_message(message):
    """
    Envoie un message via Telegram.
    
    Args:
        message (str): Le message à envoyer.
    """
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logger.info("Message envoyé avec succès")
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi du message : {e}")

def query_low_production_units():
    """
    Exécute la requête SQL optimisée pour les performances
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Création d'index recommandés (à exécuter une seule fois)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_production_unit_time 
            ON production(unit_id, timestamp DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_units_nominal 
            ON units(id, nominal)
        """)
        
        query = """
        WITH latest_prod AS (
            SELECT 
                unit_id, 
                MAX(timestamp) as last_timestamp
            FROM production
            GROUP BY unit_id
        )
        SELECT
            u.name,
            p.value,
            u.nominal
        FROM units u
        INNER JOIN latest_prod lp 
            ON u.id = lp.unit_id
        INNER JOIN production p 
            ON p.unit_id = lp.unit_id 
            AND p.timestamp = lp.last_timestamp
        WHERE 
            u.nominal > 0 
            AND p.value < 0.2 * u.nominal
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Erreur SQL : {e}")
        return []

async def generate_production_report():
    """
    Génère un rapport complet de production et retourne le message formaté
    """
    try:
        # Chargement de l'état précédent
        try:
            with open('previous_low_units.json', 'r') as f:
                previous_low_units = set(json.load(f))
        except FileNotFoundError:
            previous_low_units = set()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Requête pour obtenir la dernière valeur de production de FLAMANVILLE 3
        flamanville_query = """
        SELECT p.value
        FROM production p
        JOIN units u ON p.unit_id = u.id
        WHERE u.name = 'FLAMANVILLE 3'
        ORDER BY p.timestamp DESC
        LIMIT 1;
        """
        cursor.execute(flamanville_query)
        flamanville_result = cursor.fetchone()
        flamanville_value = flamanville_result[0] if flamanville_result else 'N/A'

        # Requête pour calculer l'âge moyen des unités < 20% de leur nominal
        age_query = """
        WITH latest_prod AS (
            SELECT 
                unit_id, 
                MAX(timestamp) AS last_timestamp
            FROM production
            GROUP BY unit_id
        ),
        low_production_units AS (
            SELECT 
                u.id,
                u.installation_date,
                p.value,
                u.nominal
            FROM units u
            INNER JOIN latest_prod lp 
                ON u.id = lp.unit_id
            INNER JOIN production p 
                ON p.unit_id = lp.unit_id 
                AND p.timestamp = lp.last_timestamp
            WHERE 
                u.nominal > 0 
                AND p.value < 0.2 * u.nominal
                AND u.installation_date IS NOT NULL
        )
        SELECT 
            AVG((JULIANDAY('now') - JULIANDAY(u.installation_date)) / 365.25) AS avg_age_low,
            COUNT(*) AS low_count,
            (SELECT AVG((JULIANDAY('now') - JULIANDAY(u2.installation_date)) / 365.25)
             FROM units u2
             WHERE u2.installation_date IS NOT NULL
               AND u2.id NOT IN (SELECT id FROM low_production_units)) AS avg_age_other,
            (SELECT COUNT(*) 
             FROM units u2
             WHERE u2.installation_date IS NOT NULL
               AND u2.id NOT IN (SELECT id FROM low_production_units)) AS other_count
        FROM low_production_units u;
        """
        cursor.execute(age_query)
        age_result = cursor.fetchone()
        avg_age_low, low_count, avg_age_other, other_count = age_result

        # Exécution de la requête complète
        query = """
        WITH 
            latest_date AS (
                SELECT MAX(timestamp) AS max_date FROM production
            ),
            total_production AS (
                SELECT SUM(p.value) as total_prod
                FROM production p
                WHERE p.timestamp = (SELECT max_date FROM latest_date)
            ),
            total_nominal AS (
                SELECT SUM(u.nominal) as total_nom
                FROM units u
            ),
            low_production_units AS (
                SELECT 
                    u.id, 
                    u.name, 
                    p.value,
                    u.nominal,
                    CAST(JULIANDAY((SELECT max_date FROM latest_date)) - 
                    JULIANDAY(COALESCE(
                        (SELECT MAX(timestamp) 
                         FROM production p3 
                         WHERE p3.unit_id = u.id 
                           AND p3.value >= 0.2 * u.nominal),
                        (SELECT MIN(timestamp) 
                         FROM production p4 
                         WHERE p4.unit_id = u.id)
                    )) AS REAL) AS days_since_above_20
                FROM production p
                JOIN units u ON p.unit_id = u.id
                WHERE p.timestamp = (SELECT max_date FROM latest_date)
                  AND p.value < 0.2 * u.nominal
            ),
            missing_units AS (
                SELECT 
                    u.id,
                    u.name,
                    (SELECT MAX(timestamp) 
                     FROM production p2 
                     WHERE p2.unit_id = u.id) AS last_record_date
                FROM units u
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM production p 
                    WHERE p.unit_id = u.id
                      AND p.timestamp >= datetime((SELECT max_date FROM latest_date), '-2 minute')
                )
            )
        SELECT 
            (SELECT max_date FROM latest_date) AS latest_date,
            (SELECT total_prod FROM total_production) AS total_production,
            (SELECT total_nom FROM total_nominal) AS total_nominal,
            (SELECT COUNT(*) FROM low_production_units) AS low_production_count,
            (SELECT json_group_array(json_object(
                'id', id, 
                'name', name, 
                'value', value,
                'nominal', nominal,
                'days_since_above_20', days_since_above_20
            )) FROM low_production_units) AS low_production_list,
            (SELECT COUNT(*) FROM missing_units) AS missing_units_count,
            (SELECT json_group_array(json_object('id', id, 'name', name, 'last_record_date', last_record_date))
             FROM missing_units) AS missing_units_list;
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()

        # Extraction des résultats
        (latest_date, total_prod, total_nominal, 
         low_count, low_list, missing_count, missing_list) = result
        
        # Calcul du facteur de charge
        load_factor = 0
        if total_nominal and total_nominal > 0:
            load_factor = (total_prod / total_nominal) * 100 if total_prod else 0

        # Formatage du message
        message = f"📊 Rapport de production - {latest_date}\n\n"
        message += f"🏭 Facteur de charge du parc : {load_factor:.1f}%\n\n"
        message += f"🏭 FLAMANVILLE 3 : {flamanville_value} MW\n\n"
        message += f"⚠️ Âge moyen des {low_count} unités < 20% de leur nominal : {avg_age_low:.2f} ans\n"
        message += f"⚠️ Âge moyen des {other_count} autres unités : {avg_age_other:.2f} ans\n\n"

        # Section unités sorties
        if low_count > 0:
            low_units = []
            for u in json.loads(low_list):  # Utiliser les données de la requête principale
                unit_name = u['name']
                value = u['value']
                nominal = u['nominal']
                percentage = (value / nominal) * 100
                days = int(u['days_since_above_20'])
                low_units.append(f"🔸 {unit_name} ({value} MW, {days} j)")
            message += "\n".join(low_units) + "\n\n"

        # Section unités manquantes
        message += f"🚨 Unités sans données : {missing_count}\n"
        if missing_count > 0:
            missing_units = [f"{u['name']} (dernier enregistrement : {u['last_record_date'] or 'Jamais'})" 
                           for u in json.loads(missing_list)]
            message += "🔹 " + "\n🔹 ".join(missing_units)
        
        return message

    except Exception as e:
        logger.error(f"Erreur génération rapport : {e}")
        return "❌ Erreur critique"

def calculate_average_age_low_production_units():
    """
    Calcule l'âge moyen des unités dont la production est < 20% de leur nominal.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Requête pour sélectionner les unités < 20% de leur nominal avec leur date d'installation
        query = """
        WITH latest_prod AS (
            SELECT 
                unit_id, 
                MAX(timestamp) AS last_timestamp
            FROM production
            GROUP BY unit_id
        )
        SELECT 
            u.name,
            u.installation_date,
            p.value,
            u.nominal
        FROM units u
        INNER JOIN latest_prod lp 
            ON u.id = lp.unit_id
        INNER JOIN production p 
            ON p.unit_id = lp.unit_id 
            AND p.timestamp = lp.last_timestamp
        WHERE 
            u.nominal > 0 
            AND p.value < 0.2 * u.nominal
            AND u.installation_date IS NOT NULL
        """
        
        cursor.execute(query)
        results = cursor.fetchall()

        # Calcul de l'âge moyen
        total_age = 0
        count = 0
        current_date = datetime.now()

        for row in results:
            name, installation_date, value, nominal = row
            installation_date = datetime.strptime(installation_date, "%Y-%m-%d")
            age = (current_date - installation_date).days / 365.25  # Convertir en années
            total_age += age
            count += 1

        conn.close()

        if count == 0:
            print("Aucune unité avec une production < 20% de leur nominal.")
            return

        average_age = total_age / count
        print(f"L'âge moyen des unités < 20% de leur nominal est de {average_age:.2f} ans.")

    except Exception as e:
        print(f"Erreur : {e}")

async def main():
    """
    Fonction principale : génère et envoie le rapport
    """
    try:
        # Génération du rapport
        report = await generate_production_report()
        
        # Envoi du rapport via Telegram
        await send_telegram_message(report)
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du rapport : {e}")

if __name__ == "__main__":
    asyncio.run(main())
    calculate_average_age_low_production_units()