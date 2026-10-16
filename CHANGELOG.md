# Changelog

What changed in each release of openforis-whisp, newest first. Every Whisp result records the version that produced it (`whisp_version` in `whisp_processing_metadata`), so this shows what would differ if a result were run again on a later version.

"Changes to results" lists output columns whose values change for some plots. "Breaking changes" lists anything a user has to change in their code or workflow. The full list of pull requests is in each [GitHub release](https://github.com/forestdatapartnership/whisp/releases).

New entries are added by the release workflow, from the "Changes to results" and "Breaking changes" headings in each pull request description.

<!-- releases below -->

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
