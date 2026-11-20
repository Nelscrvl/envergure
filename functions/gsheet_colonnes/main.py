"""Cloud Run function : liste les onglets d'un Google Sheet et leurs en-têtes.

Le Sheet de satisfaction contient des données personnelles (nom, mail) : la
fonction ne renvoie aucune ligne. Pour chaque onglet, seulement les noms de
colonnes, le nombre de lignes remplies et la première / dernière valeur de la
colonne A quand c'est une date (horodateur des réponses).

Le Sheet doit être partagé en lecture avec le compte de service de la fonction
(extraction-runner@dynamic-camp-465312-b6.iam.gserviceaccount.com).
"""
import re

import functions_framework
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
# Sheet de satisfaction des demandeurs d'emploi
SPREADSHEET_ID = "1f-gYpGhOw1f1FeWoHYNHt6E7KdH9Bw5Q7iCeDwUNMD0"
RE_DATE = re.compile(r"^\d{1,4}[/-]\d{1,2}[/-]\d{2,4}")


def _date_ou_none(valeur):
    return valeur if valeur and RE_DATE.match(valeur) else None


@functions_framework.http
def colonnes(request):
    spreadsheet_id = request.args.get("id", SPREADSHEET_ID)
    try:
        return _lire_colonnes(spreadsheet_id)
    except HttpError as e:
        # 403 : Sheet non partagé avec le compte de service ; 404 : mauvais id
        return {"erreur": e.reason, "spreadsheet_id": spreadsheet_id}, e.status_code


def _lire_colonnes(spreadsheet_id):
    creds, _ = google.auth.default(scopes=SCOPES)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False).spreadsheets()

    meta = sheets.get(
        spreadsheetId=spreadsheet_id, fields="properties.title,sheets.properties"
    ).execute()

    onglets = []
    for onglet in meta["sheets"]:
        props = onglet["properties"]
        plage = "'" + props["title"].replace("'", "''") + "'"
        entetes = sheets.values().get(
            spreadsheetId=spreadsheet_id, range=f"{plage}!1:1"
        ).execute().get("values", [])
        col_a = sheets.values().get(
            spreadsheetId=spreadsheet_id, range=f"{plage}!A2:A"
        ).execute().get("values", [])
        valeurs_a = [ligne[0] for ligne in col_a if ligne and ligne[0].strip()]
        onglets.append({
            "gid": props["sheetId"],
            "titre": props["title"],
            "colonnes": entetes[0] if entetes else [],
            "lignes_remplies": len(valeurs_a),
            "premiere_date_col_a": _date_ou_none(valeurs_a[0] if valeurs_a else None),
            "derniere_date_col_a": _date_ou_none(valeurs_a[-1] if valeurs_a else None),
        })

    return {"fichier": meta["properties"]["title"], "onglets": onglets}
