#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  9 10:49:59 2022

Lis un fichier impactv2 enrichi et le converti en format JSON pour publication sur data économie.

@author: qdimarellis-adc
"""
import json
import pandas as pd
import re
import datetime as dt


def append_to_file(data_append_to_file, outfile):
    """Ajoute l'indicateur au fichier."""
    with open(outfile, "a", encoding="utf8") as output_file:
        json.dump(data_append_to_file, output_file, ensure_ascii=False)
        output_file.write('\n')


def get_liste_points(df_: pd.DataFrame):
    """
    Parcours le dataframe et génère une liste de points contenant longitude & latitude de l'établissement,
        nom de l'entreprise, date & siren de référence.
    Relies on https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html.
    """
    return [{"latitude": lat.search(getattr(row, "coordonnees_gps")).group(0),
             "longitude": lon.search(getattr(row, "coordonnees_gps")).group(0),
             "nom": getattr(row, "Nom_Entreprise"),
             "last_date": getattr(row, "_19"),  # renamed because column name is an invalid python identifier
             "siren": getattr(row, "_3")}
            for row in df_.itertuples(index=False)]


def get_nom_from_axe_decoupe(axe_decoupe_: txt):
    if "NAF" in axe_decoupe_:
        return "Secteur de l'entreprise"
    elif "type d\'entreprise" in axe_decoupe_:
        return "Taille de l'entreprise"
    else:
        raise ValueError("Axe découpe non spécifié")


OUTFILE = "./out/impact_format_widget_" + dt.datetime.now().strftime("%d_%m_%y") + ".json"
lon = re.compile(r"(?<=,)-?\d*\.\d*$")
lat = re.compile(r"^-?\d*\.\d*")
F = "impact_v2_enrichi_09_03_22.csv"
columns_to_slice_by = ["Informations Entreprise type d'entreprise (par tranche d'effectif)",
                       "SECTION_NAF"]
df = pd.read_csv(F, sep=";")
for axe_decoupe in columns_to_slice_by:
    indicateurs = df[axe_decoupe].unique()
    for indic in indicateurs:
        df_restreint = df[df[axe_decoupe] == indic]
        data = {"code": indic,
                "nom": get_nom_from_axe_decoupe(axe_decoupe),
                "points": get_liste_points(df_restreint)}
        append_to_file(data, OUTFILE)
