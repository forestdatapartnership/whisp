  whisp
  =====
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/openforis/sepal/blob/master/license.txt)
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


  1. The Whisp App with its simple interface can be accessed [here](https://whisp.openforis.org/) or called from other software by [API](https://whisp.openforis.org/documentation/api-guide). The Whisp App currently supports the processing of up to 3,000 geometries per job. The original JS & Python code behind the Whisp App and API can be found [here](https://github.com/forestdatapartnership/whisp-app).

  2. [Whisp in Earthmap](https://whisp.earthmap.org/?aoi=WHISP&boundary=plot1&layers=%7B%22CocoaETH%22%3A%7B%22opacity%22%3A1%7D%2C%22JRCForestMask%22%3A%7B%22opacity%22%3A1%7D%2C%22planet_rgb%22%3A%7B%22opacity%22%3A1%2C%22date%22%3A%222020-12%22%7D%7D&map=%7B%22center%22%3A%7B%22lat%22%3A7%2C%22lng%22%3A4%7D%2C%22zoom%22%3A3%2C%22mapType%22%3A%22satellite%22%7D&statisticsOpen=true) supports the visualization of geometries on actual maps with the possibility to toggle different relevant map products around tree cover, commodities and deforestation. It is practical for demonstration purposes and spot checks of single geometries but not recommended for larger datasets.

  3. Datasets of any size, especially when holding more than 3,000 geometries, can be analyzed with Whisp through the [python package on pip](https://pypi.org/project/openforis-whisp/). See example [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb) for implementation with a geojson input. For further notebooks processing options see [Whisp notebooks](#whisp_notebooks).


  ## Whisp datasets <a name="whisp_datasets"></a>
  ***Whisp***  implements the convergence of evidence approach by providing a transparent and public processing flow using datasets covering the following categories:

  1) Tree and forest cover (at the end of 2020);
  2) Commodities (i.e., crop plantations and other agricultural uses at the end of 2020);
  3) Disturbances **before 2020** (i.e., degradation or deforestation until 2020-12-31);
  4) Disturbances **after 2020** (i.e., degradation or deforestation from 2021-01-01 onward).

Additional categories are specific for the timber commodity, considering a harvesting date in 2023:

  5) Primary forests in 2020;
  6) Naturally regenerating forests in 2020;
  7) Planted and plantation forests in 2020;
  8) Planted and plantation forests in 2023;
  9) Treecover in 2023;
  10) Commodities or croplands in 2023.
  11) Logging concessions;

  There are multiple datasets for each category. Find the full current [list of datasets used in Whisp here](https://github.com/forestdatapartnership/whisp/blob/main/layers_description.md).

  ### Whisp risk assessment <a name="whisp_risk"></a>

Whisp checks the plots provided by the user by running zonal statistics on them to answer the following questions:

  1) Was there tree cover in 2020?
  2) Were there commodity plantations or other agricultural uses in 2020?
  3) Were there disturbances until 2020-12-31?
  4) Were there disturbances after 2020-12-31 / starting 2021-01-01?

And specifically for the timber commodity, considering a harvesting date in 2023:

  5) Were there primary forests in 2020?
  6) Were there naturally regenerating forests in 2020?
  7) Were there planted and plantation forests in 2020?
  8) Were there planted and plantation forests in 2023?
  9) Was there treecover in 2023?
  10) Were there commodity plantations or other agricultural uses in 2023?
  11) Is it part of a logging concession?

  The Whisp algorithm outputs multiple statistical columns with disaggregated data from the input datasets, followed by aggregated indicator columns, and the final risk assessment columns.
    All output columns from Whisp are described in [this excel file](https://github.com/forestdatapartnership/whisp/blob/main/whisp_columns.xlsx)

The **relevant risk assessment column depends on the commodity** in question:

<table>
  <tr>
    <th>Commodity</th>
    <th>Risk Assessment Column</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Coffee</td>
    <td rowspan="4">Risk_PCrop</td>
    <td rowspan="4">Perennial crop deforestation risk</td>
  </tr>
  <tr><td>Cocoa</td></tr>
  <tr><td>Rubber</td></tr>
  <tr><td>Oil palm</td></tr>
  <tr>
    <td>Soy</td>
    <td>Risk_ACrop</td>
    <td>Annual crop deforestation risk</td>
  </tr>
  <tr>
    <td>Livestock</td>
    <td>Risk_Livestock</td>
    <td>Livestock driven deforestation risk</td>
  </tr>
  <tr>
    <td>Timber</td>
    <td>Risk_Timber</td>
    <td>Timber extraction deforestation risk</td>
  </tr>
</table>

  *The Whisp algorithm for **Perennial Crops** visualized:*
  ![CoE_Graphic 5](https://github.com/user-attachments/assets/007b5f50-3939-4707-95fa-98be4d56745f)

  If no treecover dataset indicates any tree cover for a plot by the end of 2020, **Whisp will categorize the deforestation risk as low.**

  If one or more treecover datasets indicate tree cover on a plot by the end of 2020, but a commodity dataset indicates agricultural use by the end of 2020, **Whisp will categorize the deforestation risk as low.**

  If treecover datasets indicate tree cover on a plot by late 2020, no commodity datasets indicate agricultural use, but a disturbance dataset indicates disturbances before the end of 2020, **Whisp will categorize the deforestation risk as <u>low</u>.** Such deforestation has happened before 2020, which aligns with the cutoff date for legislation, such as EUDR (European Union Deforestation Risk), and is therefore not considered high risk.

  Now, if the datasets under 1., 2. & 3. indicate that there was tree cover, but no agriculture and no disturbances before or by the end of 2020, the Whisp algorithm checks whether degradation or deforestation have been reported in a disturbance dataset after 2020-12-31. If they have, **Whisp will categorize the deforestation risk as <u>high</u>.** <br>
  However, under the same circumstances but with <u>no</u> disturbances reported after 2020-12-31 there is insufficient evidence and the **Whisp output will be "More info needed".** Such can be the case for, e.g., cocoa or coffee grown under the shade of treecover or agroforestry.


  ## Run Whisp python package from a notebook <a name="whisp_notebooks"></a>
  
  For most users we suggest using the Whisp App to process their plot data. But for some, using the python package directly will fit their workflow.

  An example of the package functionality can be seen in this [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb)

  For running locally (or in Sepal), see: [whisp_geojson_to_csv.ipynb](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/whisp_geojson_to_csv.ipynb) or if datasets are very large (e.g., >100,000 features), see [whisp_ee_asset_to_drive.ipynb](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/whisp_ee_asset_to_drive.ipynb)

  ### Requirements for running the package

  - A Google Earth Engine (GEE) account.
  - A registered cloud GEE project.
  - Some experience in Python or a similar language.


  ### Python package installation

  The Whisp package is available on pip
  https://pypi.org/project/openforis-whisp/


  It can be installed with one line of code:

  ```
  pip install --pre openforis-whisp
  ```


  The package relies upon the google earth engine api being setup correctly using a registered cloud project.

  More info on Whisp can be found [here](https://openknowledge.fao.org/items/e9284dc7-4b19-4f9c-b3e1-e6c142585865)



## How to add data layers to Whisp <a name="whisp_add_data"></a>



There are two main approaches:

1) Request that a layer be incorporated into the core Whisp inputs, or

2) Add your own data directly to complement the core datasets.



---



### Requesting a layer addition



If you think a particular dataset has wide applicability for Whisp users, you can request it be added to the main Whisp repository by logging it as an issue in GitHub [here](https://github.com/forestdatapartnership/whisp/issues/).



Before submitting a request, consider the following:



- Is the resolution high enough for plot-level analysis? (e.g., 30m or 10m resolution)

- Is there an indication of data quality? (e.g., accuracy assessment detailed in a scientific publication)

- Is there relevant metadata available?



---


### Adding your own data directly

The python notebooks allow the user to add custom data layers. You can edit the Prepare layers section to do this in the [Colab Notebook](https://github.com/forestdatapartnership/whisp/blob/main/notebooks/Colab_whisp_geojson_to_csv.ipynb) 
To add your own data directly you will need some coding experience as well as familiarity with Google Earth Engine.


## Contributing <a name="whisp_contribute"></a>

Contributions are welcome!  
- Fork the repo, make changes, and open a pull request.
- For adding new datasets to the codebase and for project-specific coding standards see [.github/copilot-instructions.md](.github/copilot-instructions.md)

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
- For general questions, feedback or support, email [open-foris@fao.org](mailto:open-foris@fao.org).

We welcome all feedback and contributions!


