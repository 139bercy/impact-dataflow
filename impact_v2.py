#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 09:06:03 2022

Lis un export impactv2 et l'enrichi avec codes NAF, nom & géolocalisation de la commune.

@author: qdimarellis-adc
"""
import pandas as pd
import luhn
import datetime as dt
import re


def create_dict_to_replace(df__):
    """Fonction qui crée un dictionnaire utilisable avec .replace depuis un dataframe."""
    return {df__.iloc[i_, 0]: df__.iloc[i_, 1] for i_ in df__.index}


CHUNKSIZE = 100000
OUT_DIR = "./out/IMPACT/"
REGEX_ENTREPRISE = re.compile(r"(?<=@).*(?=\..*$)")
f = "IMPACT_exportV2 20220307.xlsx"
df = pd.read_excel(f, header=None)
df.iloc[:2] = df.iloc[:2].fillna(method='ffill', axis=1)
df.iloc[2] = df.iloc[2].fillna('')
df.columns = df.iloc[:3].apply(lambda x: ' '.join([y for y in x if y]), axis=0)
df = df.iloc[3:].reset_index(drop=True)
df.dropna(subset=["Informations Entreprise UserID pseudo entré par l'entreprise"], inplace=True)
df.drop_duplicates(subset=["Informations Entreprise SIREN vérifié via l\'API"], keep='last', inplace=True)
valid = df["Informations Entreprise SIREN vérifié via l\'API"].astype(str).apply(luhn.verify)
df = df[valid]
del valid

dupes = df[df["Informations Entreprise SIREN vérifié via l\'API"].duplicated(keep=False)]

f_code_com = "./refs/StockEtablissement_utf8_01_03_2022.csv"

# etatAdministratifEtablissement == A
SE = pd.read_csv(f_code_com, usecols=["etatAdministratifEtablissement",
                                      "nomenclatureActivitePrincipaleEtablissement",
                                      "siren"])
SE_INDEX = SE.index
SE = SE[(SE["etatAdministratifEtablissement"] == "A")
        & (SE["nomenclatureActivitePrincipaleEtablissement"] == "NAFRev2")]
# nomenclatureActivitePrincipaleEtablissement == NAFRev2
dedup = ~SE["siren"].reindex(SE_INDEX).duplicated()
del SE, SE_INDEX

chunk_ite = pd.read_csv(f_code_com, chunksize=CHUNKSIZE, iterator=True,
                        dtype={"siren": 'Int64', "codeCommuneEtablissement": str,
                               "denominationUsuelleEtablissement": str,
                               "activitePrincipaleEtablissement": str},
                        usecols=["siren", "codeCommuneEtablissement", "activitePrincipaleEtablissement",
                                 "denominationUsuelleEtablissement"])

merged = []
for i, df_ in enumerate(chunk_ite):
    df_ = df_[dedup[i*CHUNKSIZE:(i+1)*CHUNKSIZE]]
    merged.append(df.merge(df_, left_on=["Informations Entreprise SIREN vérifié via l\'API"], right_on=["siren"]))
res = pd.concat(merged).reset_index(drop=True)
del merged, i, df_, chunk_ite, f_code_com

f_gps = "./refs/laposte_hexasmal.csv"
geoloc = pd.read_csv(f_gps, sep=";",
                     dtype={"Code_commune_INSEE": str, "coordonnees_gps": str},
                     usecols=["Code_commune_INSEE", "coordonnees_gps"])

gps = res.merge(geoloc.drop_duplicates(),
                left_on=["codeCommuneEtablissement"],
                right_on=["Code_commune_INSEE"])
del geoloc, f_gps

mask = gps["Informations Entreprise email"].apply(lambda x: True if '@' in x else False)

bad_mails = gps[~mask.reindex(gps.index)]
gps = gps[mask.reindex(gps.index)]
gps = gps[~(gps["Informations Entreprise type d'entreprise (par tranche d'effectif)"] == "type entreprise")]

gps["Nom_Entreprise"] = (gps["Informations Entreprise email"]
                         .apply(lambda x: REGEX_ENTREPRISE.search(x).group(0)))

gps = gps[~(gps["Nom_Entreprise"] == "gmail")]

ref_naf = "./refs/ref_naf.csv"
df_naf = pd.read_csv(ref_naf).dropna().reset_index(drop=True)
naf = create_dict_to_replace(df_naf)
gps["SECTION_NAF"] = gps["activitePrincipaleEtablissement"]
gps["activitePrincipaleEtablissement"] = gps["activitePrincipaleEtablissement"].replace(naf)

SECTION = re.compile(r"^\d.*$")
df_naf["SECTION_NAF"] = df_naf["Code"]
df_naf["SECTION_NAF"] = df_naf["SECTION_NAF"].replace(SECTION, pd.NA)
df_naf["SECTION_NAF"] = df_naf["SECTION_NAF"].replace(naf).fillna(method='ffill')
df_naf.columns = df_naf.columns.to_series().str.strip()
section_naf = create_dict_to_replace(df_naf.drop(columns="Intitulés de la  NAF rév. 2, version finale"))
gps["SECTION_NAF"] = gps["SECTION_NAF"].replace(section_naf)

gps.to_csv(OUT_DIR + "impact_v2_enrichi_" + dt.datetime.now().strftime("%d_%m_%y") + ".csv",
           sep=";", index=False)

open_data = gps[(gps["Caractéristiques E-S-G Open data Oui/non"] == "Oui")
                & (gps["Caractéristiques E-S-G publication du formulaire oui/non"] == "Oui")]

open_data.to_csv(OUT_DIR + "open_data_impact_" + dt.datetime.now().strftime("%d_%m_%y") + ".csv",
                 sep=";", index=False)

incoherent_pub_status = df[(df["Caractéristiques E-S-G Open data Oui/non"] == "Oui")
                           & (df["Caractéristiques E-S-G publication du formulaire oui/non"] == "Non")]
"""
not_in_SE = df.merge(res.drop_duplicates(subset=["Informations Entreprise SIREN vérifié via l\'API"]),
                     on=["Informations Entreprise SIREN vérifié via l\'API"],
                     how='left')
not_in_SE = not_in_SE[not_in_SE["codeCommuneEtablissement"].isna()]

dupes.to_csv(OUT_DIR + "entreprises_dupliquées_" + dt.datetime.now().strftime("%d_%m_%y") + ".csv",
             sep=";", index=False)
not_in_SE.to_csv(OUT_DIR + "entreprises_absentes_de_StockEtablissement_" + dt.datetime.now().strftime("%d_%m_%y") + ".csv",
                 sep=";", index=False)

good_name = gps.dropna(subset=["denominationUsuelleEtablissement"])

bad_mails.to_csv(OUT_DIR + "bad_mail_" + dt.datetime.now().strftime("%d_%m_%y") + ".csv",
                 sep=";", index=False)
"""
