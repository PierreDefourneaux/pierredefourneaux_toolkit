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
                raise ValueError("La variable d'environnement \"MYSQL_PASSWORD\" n'a pas été trouvée dans le .env du dossier courant.")
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
                raise ValueError("La variable d'environnement \"MYSQL_PASSWORD\" n'a pas été trouvée dans le .env du dossier courant.")

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
        database:str,
        add_auto_id: bool = True,
        primary_keys: list = None,
        foreign_keys: dict = None
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
            add_auto_id (bool): rajoute un id auto-incrémenté du nom de la table. Annulé si un argument primary_keys est déclaré.
            primary_keys (list): liste de la / des colonnes constitutives de la clé primaire de la table. Ecrase add_auto_id.
            foreign_keys (dict): dictionnaire des clés étrangères reçue par la table. Exemple : {"client_id": ("clients", "id"), "produit_id": ("produits", "id")}
        Raises:
            ValueError: Retourne l'erreur de nombre de colonnes dans la liste de types demandés.
            ValueError: Retourne l'erreur de type invalide dans la liste de types demandés.
            ValueError: Retourne l'erreur de non détéction du mot de passe MySQL dans les variables d'environnement.
        Prints:
            print(): messages de confirmation de création de la table, des colonnes puis d'insertion des lignes"""
        try:
            # Définir le nom de la table
            nom_de_la_table = re.sub(".csv","",csv)
            df = pd.read_csv(csv)
            colonnes = list(df.columns)

            # Vérifier la bonne correspondance nombre de types demandés/nombre de colonnes
            if len(colonnes) != len(types):
                raise ValueError(f"Le nombre de types demandés ({len(types)}) ne correspond pas au nombres de colonnes du CSV ({len(colonnes)}).")
            
            # Vérifier la conformité des types demandés pour Mysql. Pour rappel, booléens = TINYINT
            colonnes_types = list(zip(colonnes, types))
            valid_types = (
                "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
                "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "DATE", "DATETIME",
                "TIMESTAMP", "TIME", "YEAR", "CHAR", "VARCHAR", "TEXT",
                "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "ENUM", "JSON"
            )
            for col, t in colonnes_types:
                base_type = t.split("(")[0].upper()  # gère par exemple varchar(255)
                if base_type not in valid_types:
                    raise ValueError(
                        f"Type invalide pour la colonne `{col}` : `{t}` n'est pas un type MySQL reconnu. Types MySQL reconnus par la méthode :{valid_types}"
                    )
            # Connexion à MySQL
            mysql_password = os.getenv("MYSQL_PASSWORD")
            if not mysql_password:
                raise ValueError("La variable d'environnement \"MYSQL_PASSWORD\" n'a pas été trouvée dans le .env du dossier courant.")
            engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{host}/{database}")

            # Initialisation de la requête de création de la table
            sql_create_table = f"CREATE TABLE IF NOT EXISTS {nom_de_la_table} (\n"
            colonnes_a_creer = []

            # Append optionnel de la colonne table_id auto-incrémenté
            if add_auto_id and not primary_keys:
                colonnes_a_creer.append(f"{nom_de_la_table}_id INT AUTO_INCREMENT PRIMARY KEY")

            # Append des colonnes normales du csv
            for col, typ in zip(colonnes, types):
                col_def = f"`{col}` {typ}"
                colonnes_a_creer.append(col_def)

            # Rajouter à la fin de la requete la liste des colonnes constitutives de la clé primaire de la table
            if primary_keys:
                pk_str = ", ".join([f"`{k}`" for k in primary_keys])
                colonnes_a_creer.append(f"PRIMARY KEY ({pk_str})")

            # Rajouter à la fin de la requete le dictionnaire des clés étrangères reçues par la table
            if foreign_keys:
                for col, (ref_table, ref_col) in foreign_keys.items():
                    colonnes_a_creer.append(f"FOREIGN KEY (`{col}`) REFERENCES `{ref_table}`(`{ref_col}`)")

            # Finalisation et exécution
            sql_create_table += ",\n".join(colonnes_a_creer) + "\n);"
            with engine.connect() as conn:
                conn.execute(text(sql_create_table))
                print(f"Table `{nom_de_la_table}` créée avec succès.")

            # Insertion des lignes du csv
            for _, row in df.iterrows():
                colonnes_str = ", ".join([f"`{col}`" for col in colonnes])
                # Placeholder = protection contre les injections SQL type drop table caché dans une pseudo variable
                placeholders = ", ".join([f":{col}" for col in colonnes])
                insert_sql = text(f"""
                    INSERT INTO `{nom_de_la_table}` ({colonnes_str})
                    VALUES ({placeholders});
                """)
                valeurs = {
                    col: row[col] if pd.notna(row[col]) else None
                    for col in colonnes
                }
                with engine.begin() as conn:
                    conn.execute(insert_sql, valeurs)
            print(f"Toutes les lignes du fichier `{csv}` ont été insérées avec succès dans la table `{nom_de_la_table}`.")
        except Exception as e:
            print(f"Erreur lors de la création ou de l'insertion : {e}")
            traceback.print_exc()
            return None