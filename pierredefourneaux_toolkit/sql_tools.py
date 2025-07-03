from sqlalchemy import create_engine, text
import pandas as pd
import pymysql
import cryptography
import traceback
import re
import os
from dotenv import load_dotenv
load_dotenv()

class SqlManager:
    def __init__(self, info:str = "Repo github de Pierre Defourneaux"):
        self.info = info

    def query_with_SQLAlchemy(
            self,
            query:str,
            mysql_user:str,
            host:str,
            database:str):
        """Exécute une requête SQL sur une base de données et retourne le résultat sous forme de DataFrame.
        Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.
        Requirements = sqlachemy, pymysql, pandas, python-dotenv, cryptography
        Args:
            query (str): La requête SQL à exécuter.
            mysql_user (str): L'utilisateur MySQL pour la connection à MySQL.
            host de la base de données. Localhost, adresse IP ou nom de domaine.
            database (str): la base de données MySQL sur laquelle on souhaite opérer.
        Returns: 
            pandas.DataFrame : Un DataFrame contenant les résultats de la requête."""
        
        try:
            mysql_password = os.getenv("MYSQL_PASSWORD")
            if not mysql_password:
                raise ValueError("Le mot de passe MySQL \"MYSQL_PASSWORD\" n'a pas été trouvé dans les variables d'environnement." \
                "Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.")
            engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{host}/{database}")
            query = text(query)

            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
            return df
        except Exception as e:
            print(f"Erreur lors de l'exécution de la requête : {e}")
            traceback.print_exc()
            return None
        
    def drop_table_with_SQLAlchemy(
            self,
            table:str,
            mysql_user:str,
            host:str,
            database:str):
        """Supprime une table si elle existe via SQLAlchemy.
        Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.
        Requirements = sqlachemy, pymysql, pandas, python-dotenv, cryptography
        Args:
            table (str): table à supprimer dans la base de données MySQL
            mysql_user (str): utilisateur MySQL.
            host (str): host de la base de données. Localhost, adresse IP ou nom de domaine.
            database (str): la base de données MySQL sur laquelle on souhaite opérer.

        Raises:
            ValueError: Retourne l'erreur de non détéction du mot de passe MySQL dans les variables d'environnement.
        Prints:
            print(): message de confirmation de la suppression de la table"""
        try:
            mysql_password = os.getenv("MYSQL_PASSWORD")
            if not mysql_password:
                raise ValueError("Le mot de passe MySQL \"MYSQL_PASSWORD\" n'a pas été trouvé dans les variables d'environnement." \
                "Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.")

            engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{host}/{database}")
            dropper = text(f"drop table if exists {table}")

            with engine.connect() as connection:
                connection.execute(dropper)
                print(f"La table `{table}` a été supprimée (si elle existait).")
        except Exception as e:
            print(f"Erreur lors de la suppresion de la table : {e}")
            return None
        
    def create_table_and_insert_rows_from_csv(
        self,
        csv:str,
        types:list,
        mysql_user:str,
        host:str,
        database:str
        ):
        """Créé une table et insère les données depuis un fichier csv. Le nom de la table sera celui du csv.
        Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.
        Requirements = sqlachemy, pymysql, pandas, python-dotenv, cryptography
        Args:
            csv (str): csv dont les données seront insérées avec une création de table.
            types (list): liste des types pour chaque colonne dans l'ordre des colonnes du csv.
            mysql_user (str): utilisateur MySQL.
            host (str): host de la base de données. Localhost, adresse IP ou nom de domaine.
            database (str): la base de données MySQL sur laquelle on souhaite opérer.

        Raises:
            ValueError: Retourne l'erreur de nombre de colonnes dans la liste de types demandés.
            ValueError: Retourne l'erreur de type invalide dans la liste de types demandés.
            ValueError: Retourne l'erreur de non détéction du mot de passe MySQL dans les variables d'environnement.
        Prints:
            print(): messages de confirmation de création de la table, des colonnes puis d'insertion des lignes"""
        try:
            nom_de_la_table = re.sub(".csv","",csv)
            df = pd.read_csv(csv)

            colonnes = list(df.columns)
            if len(colonnes) != len(types):
                raise ValueError("Le nombre de types souhaités pour les colonnes de la table n'est pas égal aux nombre de colonnes dans le CSV." \
                f"({len(types)} type(s) demandé(s) dans la liste pour {len(colonnes)} colonne(s)) ")
            colonnes_types = list(zip(colonnes, types))
            #vérifier les types
            valid_types = (
                "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
                "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "DATE", "DATETIME",
                "TIMESTAMP", "TIME", "YEAR", "CHAR", "VARCHAR", "TEXT",
                "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "ENUM", "JSON"
            )
            for col, t in colonnes_types:
                base_type = t.split("(")[0].upper()  # gère par ex VARCHAR(255)
                if base_type not in valid_types:
                    raise ValueError(
                        f"Type invalide pour la colonne `{col}` : `{t}` n'est pas un type MySQL reconnu. Types MySQL reconnus par la méthode :{valid_types}"
                    )
            mysql_password = os.getenv("MYSQL_PASSWORD")
            if not mysql_password:
                raise ValueError("Le mot de passe MySQL \"MYSQL_PASSWORD\" n'a pas été trouvé dans les variables d'environnement." \
                "Il faut avoir, dans un fichier .env dans le même dossier, une variable nommée 'MYSQL_PASSWORD' stockant le mot de passe MySQL.")
            engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{host}/{database}")
            sql1 = f"""
            CREATE TABLE IF NOT EXISTS {nom_de_la_table}(
                id INT AUTO_INCREMENT PRIMARY KEY)
            """
            with engine.connect() as conn:
                conn.execute(text(sql1))
                print(f"table {nom_de_la_table} créée")
            for col_tuple in colonnes_types :
                sql2 = f"""
                    ALTER TABLE {nom_de_la_table} ADD COLUMN {col_tuple[0]} {col_tuple[1]}
                    """
                with engine.connect() as conn:
                    conn.execute(text(sql2))
                    print(f"colonne {col_tuple[0]} créée pour des types {col_tuple[1]}")
            # la syntaxe de base pour INSERT INTO est 
            # INSERT INTO nom_de_la_table (colonne1, colonne2, colonne3, ...)
            # VALUES (valeur1, valeur2, valeur3, ...);
            # Avec SQLAlchemy on peut utiliser des placeholders, cela évite les injections SQL et gère bien les types.
            # INSERT INTO clients (nom, age, ville) => On déclare les colonnes
            # VALUES (:nom, :age, :ville); => on pose les :placeholders → sûr contre les injections
            # {"nom": "Alice", "age": 28, "ville": "Nice"} puis on passe un dictionnaire
            for _, row in df.iterrows():
                colonnes_str = ", ".join([f"`{col}`" for col in colonnes])
                placeholder = ", ".join([f":{col}" for col in colonnes])
                insert_sql = text(f"""
                    INSERT INTO {nom_de_la_table} ({colonnes_str}) 
                    VALUES ({placeholder})
                """)
                valeurs = {col: row[col] if pd.notna(row[col]) else None for col in colonnes} # gestion des NaN : ils deviennent None
                with engine.begin() as conn: # engine.begin et pas engine.connnect car sinon il n'y a pas de transaction explicite(commit)
                    conn.execute(insert_sql, valeurs)
                    print(f"ligne insérée avec {valeurs}")
        except Exception as e:
            print(f"Erreur : {e}")
            return None