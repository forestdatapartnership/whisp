# Changelog

What changed in each release of openforis-whisp, newest first. Every Whisp result records the version that produced it (`whisp_version` in `whisp_processing_metadata`), so this shows what would differ if a result were run again on a later version.

"Changes to results" lists output columns whose values change for some plots. "Breaking changes" lists anything a user has to change in their code or workflow. The full list of pull requests is in each [GitHub release](https://github.com/forestdatapartnership/whisp/releases).

New entries are added by the release workflow, from the "Changes to results" and "Breaking changes" headings in each pull request description.

<!-- releases below -->

## v3.0.0a24 (2026-11-20)

A new timber decision tree, and local mode fixes.

**Changes to results:**
- risk_timber: worked out by the new timber decision tree, so it changes for many plots (on 564 test plots, low / more info / high goes from 527 / 24 / 13 to 400 / 123 / 41; the new other land gains change none of these)
- risk_timber_pathway: new column naming where each plot ended up in the timber decision tree
- Ind_05_primary_2020 adds ForTy V2; GLAD_Primary, IFL_2020 and the INPE Amazon primary forest layer are no longer read for it
- Ind_06_nat_reg_forest_2020 now reads GFT and ForTy V2 only; EUFO_2020 and the Brazilian forest layers stay in Ind_01_treecover
- Ind_07_planted_plantations_2020 now reads the planted and plantation forest layers of Ind_14_planted_forest_2020 and Ind_16_plantation_forest_2020
- Ind_10_agri_after_2020 reads five gain layers (six in Brazil), including the ESRI crop gain to 2025, instead of the ESRI crop gain to 2024 only
- GFT_primary, GFT_naturally_regenerating and GFT_planted_plantation now come from JRC Global Forest Types V2 (V1 is deprecated), which changes Ind_05_primary_2020 and Ind_06_nat_reg_forest_2020 for some plots
- Brazil: MapBiomas collection 10 replaces collection 9, and savanna is left out of the Brazil forest layer (#247), which changes risk_pcrop and risk_acrop for some Cerrado plots
- geo (each plot's shape as text): now JSON with double quotes in every mode; concurrent, sequential and the FeatureCollection path wrote it with single quotes, the way Python prints a dict
- whisp_processing_metadata (column with details of the run): in local mode it now lists unavailable_datasets when a dataset could not be read, as the other modes do, so whisp_risk also adds risk_inputs_unavailable when that dataset feeds risk

**Breaking changes:**
- The eight nBR_MapBiomas_col9_* columns are replaced by nBR_MapBiomas_col10_* columns
- Tools that rebuild risk_timber from the indicators need the new timber decision tree (TIMBER_ROOT_TREE in risk.py, or decision_tree_recipe())
- whisp_risk needs stats from this version: results saved from 3.0.0a23 or earlier must go through the stats again before rescoring
- Custom bands with theme_timber planted_plantation_2020, planted_plantation_after_2020 or logging_concession no longer change risk_timber; use planted_2020, plantation_2020 or plantation_forest_gain_after_2020
- New whisp_risk thresholds sit in the middle of its arguments, so pass everything after df by name
- Local mode now stops with an error when a download fails, instead of returning results with missing data

Pull requests: #340, #325, #324, #336, #339, #321

## v3.0.0a23 (2026-11-06)

Fixes for RADD alerts and the water flag, and a new national forest layer for Côte d'Ivoire.

**Changes to results:**
- Forest Data Partnership layers now use 2026a release data and thresholds. This could change the mapped area of cocoa, coffee, palm and/or rubber.
- Ind_01_treecover, risk_pcrop and risk_acrop for Côte d'Ivoire plots (national_codes "ci"): BNETD forest (new column nCI_Forest_bnetd_2020) now counts as tree cover, so some plots move from a low outcome to more info needed or high
- In_waterbody (flag column with True/False): True only when more than half of the plot is in water, so plots with some water but less than half now show False; points on water now show True
- Local mode: points now get the value under them for every dataset, as in the other modes (before, every dataset was 0 for a point, so risk_pcrop, risk_acrop and risk_timber always came out low). As local mode is not yet in the API, this likely only affected a few users.
- RADD_after_2020, RADD_before_2020 and RADD_year_*: only confirmed alerts from RADD's latest weekly copy (alerts RADD had dropped, and unconfirmed ones, were counted before)
- Ind_04_disturbance_after_2020, risk_pcrop, risk_acrop: where the only disturbance after 2020 was a RADD alert that is no longer counted (dropped by RADD, or not yet confirmed), plots get no and more_info_needed instead of yes and high (2 of the 50 test plots)

Pull requests: #302, #316, #318, #319, #303, #307, #301, #310

## v3.0.0a22 (2026-10-16)

**Changes to results:**
- GLAD-L_before_2020: now includes 2018-2020 alerts in regions where it was 0 (e.g. West Africa, Central America, mainland South East Asia)
- Oil_palm_Descals: now only plantations with a planting year of 2020 or earlier (those dated 2021 were included by mistake)
- RADD_before_2020 and GLAD-S2_before_2020: alerts dated 31 December 2020 now count here
- nBR_MapBiomas_col9_pc_2020: now citrus and other perennial crops (it used to repeat the coffee and palm oil columns)
- Country, ProducerCountry, Admin_Level_1: the region covering most of the plot, so some plots across a border change
- Local mode: statistics now stay on the right plot (with more than 25 plots they could end up on another plot)

**Breaking changes:**
- If any plot has no geometry, the run now stops and says which plots (before, such plots were dropped without a message)

Pull requests: #300, #287

## v3.0.0a21 (2026-10-13)

**Changes to results:**
- Ind_02_commodities: now needs more than 50% of the plot (was 10%), so fewer plots get a low outcome from a small commodity share

Pull requests: #290

## v3.0.0a20 (2026-10-13)

**Changes to results:**
- Drive export: plots with no external id now get an empty external_id cell (the run used to stop)

Pull requests: #285, #279, #286, #283, #289, #284

## v3.0.0a19 (2026-10-06)

**Changes to results:**
- EUFO_2020: now JRC GFC2020 version 4 (was version 3); less forest on some plots, notably in tree-crop landscapes

**Breaking changes:**
- mode='legacy' and whisp_formatted_stats_geojson_to_df_legacy are removed; use whisp_formatted_stats_geojson_to_df with mode='sequential' or 'concurrent'

Pull requests: #282, #273, #272, #271, #274

## v3.0.0a18 (2026-09-28)

**Changes to results:**
- Ind_03_disturbance_before_2020: now needs more than 50% of the plot (was 10%)
- DIST_after_2020: back in the output and in the risk trees
- Rubber_RBGE_2020 replaces Rubber_RBGE (the Ahrends et al. 2026 map)
- A dataset that can't be read is now left out and listed in whisp_processing_metadata, instead of stopping the run

**Breaking changes:**
- The Rubber_RBGE column is now Rubber_RBGE_2020

Pull requests: #269, #256, #266, #259, #262, #267, #268, #265, #264, #260, #270, #261

## v3.0.0a17 (2026-09-15)

**Changes to results:**
- SBTN_natural_2020: new output column (not used in risk)
