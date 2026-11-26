  whisp
  =====
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
  [![Data Protection Policy](https://img.shields.io/badge/Data_Protection_and_Privacy-FAO-lightblue.svg)](https://www.fao.org/contact-us/data-protection-and-privacy/en/)
  [![DOI](https://img.shields.io/badge/DOI-10.4060%2Fcd0957en-brightgreen.svg)](https://doi.org/10.4060/cd0957en)


  ![Whisp_OpenForis_Banner_Github](https://github.com/user-attachments/assets/84f002fe-1848-46a1-814d-3949c22728cb)

  ## Convergence of Evidence <a name="whisp_coe"></a>
  ***Whisp*** stands for "***Wh****at* ***is*** *in that* ***p****lot*"?

  Numerous publicly available Earth Observation maps provide data on tree cover, land use, and forest disturbances. However, these maps often differ from one another because they use various definitions and classification systems. As a result, no single map can provide a complete picture of any specific area. To address this issue, the [Forest Data Partnership (FDaP)](https://www.forestdatapartnership.org) and the [AIM4Forests Programme](https://www.fao.org/in-action/aim4forests/en/) advocate for the **Convergence of Evidence** approach.

  The Forest Data Partnership promotes this approach for forest and commodities monitoring, assuming that
  - no single source of geospatial data can tell the whole story around any given plot of land;
  - all the existing, published and available datasets contribute to telling that story.
  <br><br>


  ## Contents
  - [Whisp pathways](#whisp_pathways)
  - [Whisp datasets](#whisp_datasets)
  - [Whisp notebooks](#whisp_notebooks)
  - [Add data layers](#whisp_add_data)
  - [Contribute to the code](#whisp_contribute)
  - [Code of conduct](#whisp_conduct)
  - [Feedback](#whisp_feedback)

  <br>

  ## Whisp pathways <a name="whisp_pathways"></a>
  ***Whisp*** can currently be used directly or implemented in your own code through three different pathways:


  1. The [Whisp App](https://whisp.openforis.org/) has a simple interface for submitting plots, and its [API](https://whisp.openforis.org/api/docs) lets other software do the same. It handles up to 10,000 geometries per job ([current limits](https://whisp.openforis.org/api/config)) and needs no Google Earth Engine account, though the API can run on your own if you have one. API use is governed by the [Terms of Service](https://www.openforis.org/whisp-terms/). The code for the App and API is in a separate repository, [whisp-app](https://github.com/forestdatapartnership/whisp-app).

  2. [Whisp in Earthmap](https://whisp.earthmap.org/?aoi=WHISP&boundary=plot1&layers=%7B%22CocoaETH%22%3A%7B%22opacity%22%3A1%7D%2C%22JRCForestMask%22%3A%7B%22opacity%22%3A1%7D%2C%22planet_rgb%22%3A%7B%22opacity%22%3A1%2C%22date%22%3A%222020-12%22%7D%7D&map=%7B%22center%22%3A%7B%22lat%22%3A7%2C%22lng%22%3A4%7D%2C%22zoom%22%3A3%2C%22mapType%22%3A%22satellite%22%7D&statisticsOpen=true) supports the visualization of geometries on actual maps with the possibility to toggle different relevant map products around tree cover, commodities and deforestation. It is practical for demonstration purposes and spot checks of single geometries but not recommended for larger datasets.

  3. The [Whisp python package](https://pypi.org/project/openforis-whisp/) is the code in this repository. It runs the analysis behind the Whisp App and API, described below, and can be used directly for datasets of any size, especially those with more than 10,000 geometries, or for [adding your own data layers](#whisp_add_data). The [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb) shows an example with a geojson input, and [Whisp notebooks](#whisp_notebooks) has further options.

  Other tools build on Whisp, such as the [QGIS plugin](https://plugins.qgis.org/plugins/whisp_plugin/), the [Open Foris Ground](https://www.openforis.org/ground/) mobile app and [dashboard templates](https://github.com/forestdatapartnership/whisp-dashboards); see the [Open Foris Whisp page](https://www.openforis.org/solutions/whisp/) for the full set.

  Whisp is open source under the MIT License, which allows commercial use. Data layers come from third-party providers with their own terms (see the [layers description](https://github.com/forestdatapartnership/whisp/blob/main/layers_description.md)).


  ## Whisp datasets <a name="whisp_datasets"></a>
  ***Whisp***  implements the convergence of evidence approach by providing a transparent and public processing flow. For each plot it answers the questions below from groups of datasets (the full current [list of datasets used in Whisp is here](https://github.com/forestdatapartnership/whisp/blob/main/layers_description.md)); each answer is an indicator column in the output.

  ### Whisp risk assessment <a name="whisp_risk"></a>

  For all commodities:

  1) Was there tree cover in 2020? (`Ind_01_treecover`)
  2) Were there commodities or other agricultural uses in 2020? (`Ind_02_commodities`)
  3) Were there disturbances (degradation or deforestation) until 2020-12-31? (`Ind_03_disturbance_before_2020`)
  4) Were there disturbances from 2021-01-01 onward? (`Ind_04_disturbance_after_2020`)

  And for timber (wood), comparing 2020 with each map's latest year (2024 or 2025 at present):

  5) Was there primary forest in 2020? (`Ind_05_primary_2020`)
  6) Was there naturally regenerating forest in 2020? (`Ind_06_nat_reg_forest_2020`)
  7) Was there planted forest in 2020? (`Ind_14_planted_forest_2020`)
  8) Was there plantation forest in 2020? (`Ind_16_plantation_forest_2020`)
  9) Was there other land in 2020, e.g. built-up, water or mines? (`Ind_12_other_land_2020`)
  10) Was there cropland in 2020? (`Ind_15_cropland_2020`)
  11) Did agriculture expand after 2020? (`Ind_10_agri_after_2020`)
  12) Was primary or naturally regenerating forest converted into plantation forest after 2020? (`Ind_17_plantation_forest_gain_after_2020`)
  13) Did the plot become other land (e.g. built-up or water; in Brazil also mining) after 2020? (`Ind_13_other_land_gain_after_2020`)

  The output also keeps four older columns, `Ind_07_planted_plantations_2020`, `Ind_08_planted_plantations_after_2020`, `Ind_09_treecover_after_2020` and `Ind_11_logging_concession_before_2020` for existing users; no decision tree reads them.

  Each question is answered "yes" for a plot when any one of the relevant datasets covers more than a set share of the plot area. By default this share is 10% for all indicators except commodities in 2020, disturbances before 2020-12-31 and, for timber, cropland in 2020, other land in 2020 and other land after 2020, where it is 50%: because a "yes" to any of these lowers the risk category (see the logic below), Whisp only counts them when they cover most of the plot rather than a small corner. For commodities this also guards against commodity maps over-predicting (commission errors), where a small patch of mapped commodity on a forested plot would otherwise be enough for a low risk outcome. For point locations any non-zero value counts. All thresholds can be changed via the `ind_N_pcent_threshold` arguments of `whisp_risk` in the python package.

  The Whisp algorithm outputs multiple statistical columns with disaggregated data from the input datasets, followed by aggregated indicator columns, and the final risk assessment columns.
    All output columns from Whisp are described in the [result fields reference](https://whisp.openforis.org/docs/reference/result-fields).

  Occasionally a dataset can't be read from Earth Engine (an asset has moved or gone empty). Rather than failing the whole run, Whisp skips it and records its name in the `whisp_processing_metadata` column, under `unavailable_datasets`. If a skipped dataset is one the risk assessment uses, `whisp_risk` warns and also lists it under `risk_inputs_unavailable` in the same column, meaning the risk columns were worked out without that dataset.

The **relevant risk assessment column depends on the commodity** in question:

<table>
  <tr>
    <th>Commodity</th>
    <th>Risk Assessment Column</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Coffee</td>
    <td rowspan="4"><code>risk_pcrop</code></td>
    <td rowspan="4">Perennial crop deforestation risk</td>
  </tr>
  <tr><td>Cocoa</td></tr>
  <tr><td>Rubber</td></tr>
  <tr><td>Oil palm</td></tr>
  <tr>
    <td>Soy</td>
    <td><code>risk_acrop</code></td>
    <td>Annual crop deforestation risk</td>
  </tr>
  <tr>
    <td>Cattle</td>
    <td><code>risk_cattle</code></td>
    <td>Cattle-driven deforestation risk (coming soon)</td>
  </tr>
  <tr>
    <td>Timber</td>
    <td><code>risk_timber</code></td>
    <td>Timber deforestation and degradation risk</td>
  </tr>
</table>

Each risk column holds one of the values `low`, `high` or `more_info_needed` (shown as "Low", "High" and "More info needed" in the Whisp App).

Statistics are no longer decimal-rounded by default before risk is calculated; indicators and risk can therefore change near thresholds, particularly for small plots. `format_stats_dataframe` also defaults to no decimal rounding; pass `decimal_places` explicitly when rounding is needed. Apply presentation rounding after calculating risk. The output schema still uses `float32` for many values, so values extremely close to a threshold remain subject to floating-point precision limits. Previously rounded exports cannot recover their original precision.

  **Note:** the specific datasets named in the diagrams below are examples only and may not all be reflected in the current codebase. For the datasets currently used, see the [layers description](https://github.com/forestdatapartnership/whisp/blob/main/layers_description.md).

  *The Whisp algorithm for **Perennial Crops** visualized:*
  ![CoE_Graphic 5](https://github.com/user-attachments/assets/007b5f50-3939-4707-95fa-98be4d56745f)

  For perennial crops, Whisp assesses a deforestation risk for a plot of land based on the datasets for questions 1–4 on the presence of tree cover, agricultural use, and disturbances before and after 2020. The end-of-2020 date is used as the cutoff for regulatory frameworks such as the EU Deforestation Regulation (EUDR).

  If no treecover dataset indicates any tree cover for a plot by the end of 2020, **Whisp will categorize the deforestation risk as low.**

  If one or more treecover datasets indicate tree cover on a plot by the end of 2020, but a commodity dataset indicates agricultural use by the end of 2020, **Whisp will categorize the deforestation risk as low.**

  If one or more treecover datasets indicate tree cover on a plot by the end of 2020, no commodity datasets indicate agricultural use, but a disturbance dataset indicates disturbances before the end of 2020, **Whisp will categorize the deforestation risk as <u>low</u>.** This approach accounts for the characteristics of some perennial crops, which can be established under significant canopy cover (e.g. coffee, cocoa); disturbances prior to 2020 are interpreted as potential evidence of crop establishment before the end of 2020, and thus not considered high risk. Because this step lowers the risk category, it uses a higher default threshold (50% of the plot area) than most other indicators (10%).

  Now, if the datasets for questions 1–3 indicate that there was tree cover, but no agriculture and no disturbances before or by the end of 2020, the Whisp algorithm checks whether degradation or deforestation have been reported in a disturbance dataset after 2020-12-31. If they have, **Whisp will categorize the deforestation risk as <u>high</u>.** <br>
  However, under the same circumstances but with <u>no</u> disturbances reported after 2020-12-31 there is insufficient evidence and the **Whisp output will be "More info needed".** Such can be the case for, e.g., cocoa or coffee grown under the shade of treecover or agroforestry.


  *The Whisp algorithm for **Annual Crops** visualized:*
  ![annual_decision_tree](https://github.com/user-attachments/assets/0148736f-d9a1-4511-a8f8-8b32287483cb)

  For annual crops, Whisp applies the same general framework based on tree cover, commodity, and disturbance datasets. However, disturbances before 2020-12-31 are not considered relevant, since annual crops are not typically established under significant canopy cover.

  If no tree cover is indicated for a plot by the end of 2020, **Whisp will categorize the deforestation risk as low.**

  If one or more tree cover datasets indicate tree cover on a plot by the end of 2020, and commodity datasets indicate agricultural use by the same date, **Whisp will categorize the deforestation risk as low.**

  If tree cover is present and no agricultural use is recorded by the end of 2020, Whisp evaluates disturbance datasets after 2020-12-31:
  - If deforestation or degradation is detected, **Whisp will categorize the deforestation risk as high.**
  - If no such disturbances are detected, the **Whisp output will be "More info needed"**, as the available evidence is insufficient for a definitive classification.

  *The Whisp algorithm for **Timber** visualized:*
  ![timber_graphic](https://github.com/user-attachments/assets/497d4137-d4c3-4e35-a87b-3abc2977f1ce)

  For timber, Whisp also assesses **degradation risk**: primary or naturally regenerating forest converted into plantation forest (tree crops such as oil palm and rubber count as agriculture, not plantation forest). "Forest in 2020" is wider than tree cover for crops (tree cover or any forest type), because much timber comes from planted and plantation forests that some forest maps leave out. Each change after 2020 is measured within one dataset.

  - Not forest in 2020: other land or agriculture in 2020 gives **low**; otherwise **more info needed**.
  - Forest in 2020 and agriculture after 2020: **high** (deforestation).
  - Primary forest: converted into plantation forest gives **high** (degradation); became other land gives **low**; disturbance after 2020 gives **more info needed**; otherwise **low**.
  - Naturally regenerating or planted forest: converted into plantation forest gives **high**; otherwise **low**.
  - Plantation forest: **low**.
  - Forest with no forest type mapped: **more info needed**.

  The decision trees are defined in [risk.py](https://github.com/forestdatapartnership/whisp/blob/main/src/openforis_whisp/risk.py) (`DECISION_TREES`; the timber one is `TIMBER_ROOT_TREE`). `openforis_whisp.decision_tree_recipe()` returns each one as data (the questions, the columns each reads, the end points, and each indicator's default threshold and datasets), for anyone rebuilding or displaying a decision tree.

  The decision trees will change over time as data and methods improve (as the timber one has in 3.0.0a24). Indicator and risk columns may change too: new columns are added alongside the old ones, which are kept for a while and flagged as deprecated in the release notes before they are removed.



  ## Run Whisp python package from a notebook <a name="whisp_notebooks"></a>

  For most users we suggest using the Whisp App to process their plot data. But for some, using the python package directly will fit their workflow.

  An example of the package functionality can be seen in this [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb)

  For running locally on your machine (or in Sepal), see: [whisp_geojson_to_csv.ipynb](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/whisp_geojson_to_csv.ipynb) or if datasets are very large (e.g., >100,000 features), you could also try [whisp_ee_asset_to_drive.ipynb](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/whisp_ee_asset_to_drive.ipynb).

  ### Requirements for running the package <a name="whisp_requirements"></a>

  - A Google Earth Engine (GEE) account.
  - A registered cloud GEE project.
  - Some experience in Python or a similar language.


  ### Python package installation

  The Whisp package is on [PyPI](https://pypi.org/project/openforis-whisp/) (changes in the [release notes](https://github.com/forestdatapartnership/whisp/releases)). Install it with:

  ```
  pip install --pre openforis-whisp
  ```


  The package relies upon the google earth engine api being setup correctly using a registered cloud project.

  More info on Whisp can be found [here](https://openknowledge.fao.org/items/e9284dc7-4b19-4f9c-b3e1-e6c142585865)



## How to add data layers to Whisp <a name="whisp_add_data"></a>

There are two main approaches:

1) Request that a layer be incorporated into the core Whisp inputs, or

2) Add your own data directly to complement the core datasets.

### Requesting a layer addition

If you think a particular dataset has wide applicability for Whisp users, you can request it be added to the main Whisp repository by logging it as an issue in GitHub [here](https://github.com/forestdatapartnership/whisp/issues/).

Before submitting a request, consider the following:

- Is the resolution high enough for plot-level analysis? (e.g., 30m or 10m resolution)

- Is there an indication of data quality? (e.g., accuracy assessment detailed in a scientific publication)

- Is there relevant metadata available?

### Adding your own data directly

The python notebooks allow the user to add custom data layers. You can edit the Prepare layers section to do this in the [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb)
To add your own data directly you will need some coding experience as well as familiarity with Google Earth Engine.


## Contributing <a name="whisp_contribute"></a>

Contributions are welcome!
- Fork the repo, make changes, and open a pull request.
- For adding new datasets to the codebase and for project-specific coding standards see [AGENTS.md](AGENTS.md)

### Developer setup <a name="whisp_dev_setup"></a>

You need a Google Earth Engine account and a registered cloud project (see [requirements](#whisp_requirements)). Then:

```
git clone https://github.com/forestdatapartnership/whisp.git
cd whisp
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e .
pip install pytest pre-commit
cp .env.template .env            # then set PROJECT to your Earth Engine cloud project
earthengine authenticate         # one-off browser sign-in
pre-commit install
pytest
```

Notes:
- `.env` is gitignored. Keep any keys or project names there or in environment variables, never in tracked files or notebook outputs.
- The tests run real Earth Engine computations on a small fixture (about 50 plots), so they need the credentials and project above and take around a minute. The same tests run as a pre-commit hook on every commit.
- If you want the tests to sign you out of Earth Engine afterwards (so every run starts with a fresh browser sign-in and no credentials stay on disk), set `WHISP_CLEAR_EE_CREDS=1` in `.env`. Off by default.
- Formatting is done by the pinned `black` pre-commit hook; let it reformat your files rather than running a different `black` version by hand.

## Code of Conduct <a name="whisp_conduct"></a>

**Purpose**
We are dedicated to maintaining a safe and respectful environment for all users. Harassment or abusive behavior will not be tolerated. <br>

**Scope**
This Code applies to all interactions on the repository and on the app.

**Expectations** <br>
*- Respect others:* Treat all contributors and users with courtesy and kindness. <br>
*- Constructive communication:* Engage respectfully, even in disagreements. <br>
*- Protect privacy:* Do not share personal information without consent.

**Prohibited Conduct** <br>
*- Harassment:* Unwanted or abusive communication, stalking, threats, or bullying.<br>
*- Discrimination:* Any form of hate speech or exclusion based on race, gender, orientation, or other identities.<br>
*- Inappropriate Content:* Posting offensive, harmful, or explicit material.

**Reporting** <br>
Users can report violations of this Code of Conduct confidentially by contacting the Open Foris team at
[open-foris@fao.org](mailto:open-foris@fao.org).


## Feedback <a name="whisp_feedback"></a>
- For issues or feature requests [open a GitHub issue](https://github.com/forestdatapartnership/whisp/issues).
- For general questions, feedback or support, see the [Whisp FAQs](https://www.openforis.org/whisp-faqs/), ask on the [Open Foris forum](https://openforis.support/tags/whisp/) or email [open-foris@fao.org](mailto:open-foris@fao.org).

We welcome all feedback and contributions!
