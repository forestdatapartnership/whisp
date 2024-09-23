whisp 
=====
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/openforis/sepal/blob/master/license.txt)
[![Privacy Policy](https://img.shields.io/badge/Privacy_Policy-FAO-lightblue.svg)](https://www.fao.org/contact-us/privacy-policy-applications-use/en)
[![DOI](https://img.shields.io/badge/DOI-10.4060%2Fcd0957en-brightgreen.svg)](https://doi.org/10.4060/cd0957en)



![Whisp_OpenForis_Banner_Github](https://github.com/user-attachments/assets/fe7a6505-2afa-40a2-8125-23f8d153be51)

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
- [System setup](#whisp_setup)
- [Add data layers](#whisp_add_data)
- [Contribute to the code](#whisp_contribute)
- [Code of conduct](#whisp_conduct)

<br>

## Whisp pathways <a name="whisp_pathways"></a>
***Whisp*** can currently be used directly or implemented in your own code through three different pathways:


1. The Whisp App with its simple interface can be used [right here](https://whisp.openforis.org/) or called from other software by [API](https://whisp.openforis.org/documentation). The Whisp App currently supports the processing of up to 500 geometries per job. The original JS & Python code behind the Whisp App and API can be found [here](https://github.com/forestdatapartnership/whisp-app).

2. [Whisp in Earthmap](https://whisp.earthmap.org/?aoi=WHISP&boundary=plot1&layers=%7B%22CocoaETH%22%3A%7B%22opacity%22%3A1%7D%2C%22JRCForestMask%22%3A%7B%22opacity%22%3A1%7D%2C%22planet_rgb%22%3A%7B%22opacity%22%3A1%2C%22date%22%3A%222020-12%22%7D%7D&map=%7B%22center%22%3A%7B%22lat%22%3A7%2C%22lng%22%3A4%7D%2C%22zoom%22%3A3%2C%22mapType%22%3A%22satellite%22%7D&statisticsOpen=true) supports the visualization of geometries on actual maps with the possibility to toggle different relevant map products around tree cover, commodities and deforestation. It is practical for demonstration purposes and spot checks of single geometries but not recommended for larger datasets.

3. Datasets of any size, especially when holding more than 500 geometries, can be "whisped" through the [Jupyter Notebook](whisp_feature_collection.ipynb) in this repository. They can either be uploaded as GeoJSONs or accessed through GEE assets. For the detailed procedure please go to the section [Whisp notebooks](#whisp_notebooks).

<br>

## Whisp datasets <a name="whisp_datasets"></a>
***Whisp***  implements the convergence of evidence approach by providing a transparent and public processing flow using datasets covering the following categories:

1) Tree and forest cover (at the end of 2020);
2) Commodities (i.e., crop plantations and other agricultural uses at the end of 2020);
3) Disturbances **before 2020** (i.e., degredation or deforestation until 2020-12-31);
4) Disturbances **after 2020** (i.e., degredation or deforestation from 2021-01-01 onward).

There are multiple datasets for each category. Find the full current [list of datasets used in Whisp here](https://github.com/forestdatapartnership/whisp/blob/main/layers_description.md).
 Whisp checks the plots provided by the user by running zonal statistics on them to answer the following questions:

1) Was there tree cover in 2020?
2) Were there commodity plantations or other agricultural uses in 2020?
3) Were there disturbances until 2020-12-31?
4) Were there disturbances after 2020-12-31 / starting 2021-01-01?

If no treecover dataset indicates any tree cover for a plot by the end of 2020, **Whisp will deem the deforestation risk as low.** 

If one or more treecover datasets indicate tree cover on a plot by the end of 2020, but a commodity dataset indicates agricultural use by the end of 2020, **Whisp will deem the deforestation risk as low.**

If treecover datasets indicate tree cover on a plot by late 2020, no commodity datasets indicate agricultural use, but a disturbance datasets indicates disturbances before the end of 2020, **Whisp will deem the deforestation risk as <u>low</u>.** Such deforestation has happened before the EUDR cutoff date and therefore does not count as high risk for the EUDR.

Now, if the datasets under 1., 2. & 3. indicate that there was tree cover, but no agriculture and no disturbances before or by the end of 2020, the Whisp algorithm checks whether degredation or deforestation have been reported in a disturbance dataset after 2020-12-31. If they have, **Whisp will deem the deforestation risk as <u>high</u>.** <br>
However, under the same circumstances but with <u>no</u> disturbances reported after 2020-12-31 there is insufficient evidence and the **Whisp output will be "More info needed".** Such can be the case for, e.g., cocoa or coffee grown under the shade of treecover or agroforestry.


*The Whisp algorithm visualized:*
![Kopie von whisp_decision_tree_20240909](https://github.com/user-attachments/assets/6a49dac8-d3b0-4137-871e-37a879d0e173)

## Run Whisp through Jupyter Notebooks <a name="whisp_notebooks"></a>

### Requirements

- A [Sepal account](https://sepal.io/-/process);
- a [Google Earth Engine (GEE) account](https://code.earthengine.google.com/);
- a registered cloud GEE project;
- some experience in Python or a similar language.

The Python notebooks are currently set up to run in Sepal and to focus on polygon data provided by the user or [GeoIDs](https://openknowledge.fao.org/server/api/core/bitstreams/b54cabe9-7ecb-46bf-810d-e3f028818067/content) that allow the notebooks to access polygons stored and registered in the [Asset Registry](https://github.com/agstack/asset-registry). NB: We value your feedback in terms of what input data we should support. 

If your data is available as a feature collection and the GitHub repo is cloned, you are ready to start processing. We suggest first familiarizing yourself with running the notebooks using the default inputs in the notebook code. This will allow you to understand the expected outputs and the general functionality, as well as check if the setup worked successfully.

### Run WHISP on a Feature Collection

1. Open Jupyter Lab (see Apps).
2. Open the notebook `whisp_feature_collection.ipynb` from inside your Whisp folder. If you wish to view the original in GitHub, see [here](https://github.com/forestdatapartnership/whisp/blob/main/whisp_feature_collection.ipynb).
3. To run the notebook cells, press the Play icon, or use the Shift + Enter shortcut. For more info on Jupyter and controlling notebooks, see [here](https://jupyter.org/try-jupyter/lab/).
4. You can change the ROI (region of interest) to point to your own feature collection data instead of the default input feature collection asset.
5. The outputs from the notebook will appear in your output folder (outside the Whisp repository folder).
6. Outputs from the process include a CSV file `whisp_output_table.csv`.
7. NB If the processing job is large (currently defined as such if it has over 500 features), the Whisp output may be sent to your Google drive, or stored as GEE assets. We are developing functionality to upload

### Adding Geo IDs to your results (Optional)
1. After the main Whisp process, there are some optional steps for adding geo_ids to each of the features in your feature collection. This uses functionality from the Asset Registry API (see https://asset-registry.agstack.org/ for details).
2. Currently, the process of registering a plot takes about 2-3 seconds. The speed of this external API should increase in the future. In the meantime, we use the approach of creating a lookup table (a temporary CSV called `temp_geo_id_lookup.csv`) containing geo-ids for each feature.  This can then be joined to the CSV outputs of the Whisp process (i.e., `whisp_output_table_w_risk.csv`) via a common id. Currently the common id is the `system:index` column as this is a property present in every GEE feature collection. The notebook is set up to create a new CSV with the Geo ID column appended, called: `whisp_output_table_w_risk_w_geo_ids.csv`. The output name is easily changed, e.g. to overwrite the original output table, if preferred.
3.  This file stores geo_ids along with a unique id (`system:index`) from the feature collection. This system:index column can then be used to join the Geo IDs column on to the Whisp output table (`whisp_output_table.csv`). Similarly the lookup can also be used to join Geo IDs to the input feature collection.
4. This approach is useful when registering large numbers of features/plots, as if the process is interrupted (e.g., if a bug occurs or the SEPAL instance times out), the lookup CSV still contains all the geo ids processed until this point. Therefore, the process automatically continues from where it stopped when this cell is rerun.
5. This lookup table of Geo IDs is then appended to the results from Whisp.
   
### Adding risk indicators to your results  
6. Finally, functions at the end of the notebook allow the user to add risk indicators to the table. NB: these risk indicators are still at the experimental stage and aim to support compliance with deforestation-related regulations. 

### Intermediate Output

- A temporary CSV called `temp_geo_id_lookup.csv` contains a geo id column and a `system:index` column.

### Output 1

- A CSV called `whisp_output_table.csv` contains results from the whisp processing.

### Output 2

- A CSV called `whisp_output_table_w_risk.csv` contains results from the whisp processing and risk indicators.

### Output 3

- A CSV called `whisp_output_table_w_risk_w_geo_ids.csv` contains results from the whisp processing, risk indicators, and a column for the newly registered geo ids.

### Whisping a List of Geo IDs

1. Open Jupyter Lab (see Apps).
2. Open the following notebook from your files `whisp_geo_id.ipynb`. If you wish to view the original on GitHub, see [here](link_to_github).
3. NB: This notebook assumes you have registered some plots already in the AgStack Asset Registry and have obtained the corresponding Geo IDs.
4. Run the cells as with the previous notebook.
5. This notebook requires a list of Geo IDs.
6. Each Geo ID corresponds to a unique boundary in the Asset Registry. The functions in this notebook fetch the boundaries and turn each Geo ID into a feature stored in a feature collection.
7. The feature collection is then run in the same way as with the previous notebook, producing Whisp summary statistics as a series of CSV tables.

### Parameters Folder

A folder containing a series of Python scripts and a CSV. These files are used to define various parameters used in the analysis.

Key files include:
- `lookup_gee_datasets.csv` contains the list of input datasets, the order they will be displayed, which ones are to be excluded from the current analysis, and which ones are shown as flags (i.e., shown as presence or absence instead of figures for area/percentage coverage of the plot).
- `config_runtime.py` contains parameters that the user can tweak, e.g., file and column names.

### Modules Folder

Various functions for the main Whisp analysis along with some for interacting with the AgStack Asset Registry.

Key files:
- `datasets.py` contains a series of functions related to the creation of a single multiband GEE image to be used in the Whisp summary statistics analysis.
- `stats.py` contains functions to run the Whisp analysis for each of the various datasets and to provide results for coverage of each plot as a percentage (or as an area in hectares).

<br>

## Setting Up Your System <a name="whisp_setup"></a>

### Setting Up SEPAL

SEPAL is closely linked to Google Earth Engine (GEE), a Google-powered Earth-observation cloud-computing platform, as it builds in many of its functionalities. Currently, you will need to have connected SEPAL and GEE accounts. SEPAL provides a stable processing environment and allows you to link up with your Google account saving time with permissions. Currently, we are supporting the use within SEPAL, but you can run the scripts outside of SEPAL if required, although this will require extra code to account for the various dependencies SEPAL already has built in.

1. Login to SEPAL.
2. Start an instance (see Terminal info) to provide you with free processing power.
3. If you don’t have SEPAL set up:
    - To create a SEPAL account, please follow the registration steps described [here](https://docs.sepal.io/en/latest/setup/register.html) and then familiarize yourself with the tool by exploring its interface.
    - To create a Google Earth Engine (GEE) account, please follow these steps and don’t forget to initialize the home folder.

### Setting Up the Whisp GitHub Repository

1. Make sure you have a GitHub account set up.
2. To run the Whisp notebooks in SEPAL, you need to copy the Whisp repository into your SEPAL files. The notebooks rely on functions and parameters that are stored in other files in this repository.
3. To clone (i.e., copy) the GitHub repository so that it is accessible in SEPAL, type into the SEPAL terminal:

    ```sh
    git clone https://github.com/forestdatapartnership/whisp.git
    ```

4. If this works, on the left-hand pane you should now be able to view a Whisp folder containing Notebooks along with other supporting files and folders.

### Setting Up Your Input Plot Data

#### Converting Plot Data into a Feature Collection

Whisp summary statistics are processed using the cloud computing platform Google Earth Engine (GEE). To do this, plot data needs to be in feature collection format.

#### Conversions via the GEE Python API: ‘On the Fly’ Processing

To help, Whisp notebooks support conversion on the fly from geosjon. This is possible in the whisp_feature_collection.ipynb notebook where you can choose between a pre-existing GEE asset or Goejson input. Further conversions including compatability with shapefiles are found in the the "data_conversion.ipynb" notebook. We are in the process of making Whisp more resilient to differences in geojson input formats.

NB Some of these conversions are carried out adapting functions from the [geemap](https://geemap.org/) package. For those that are interested in other formats, such as KML, WKT etc, geemap provides additional options (although currently functionality varies due to recent updates in dependencies for this package).

#### Conversions via GEE Code Editor: Uploading Shapefile as a GEE Asset

As large or detailed polygon data may cause conversion errors, for more reliable functionality, you can also upload a shapefile straight into the GEE code editor where it is stored as a feature collection asset. This is documented [here](https://developers.google.com/earth-engine/guides/table_upload#:~:text=4326%20before%20uploading.-,Upload%20a%20Shapefile,on%20your%20local%20file%20system/) as well as below.

1. Go to Assets in the top left panel in the Earth Engine Code Editor page.
2. Clicking on it will open the Asset Manager.
3. Select New.
4. You will have several choices. Choose Vector (Shapefiles).
5. A pop-up window will appear. Navigate to the location of your data.
6. In the pop-up window, select the file you want to upload from your computer.
7. You can upload the vector data in a compressed mode as a .zip file. If not, remember that a .shp file alone is not sufficient and must be accompanied by other files describing the vector data. Any file errors will be highlighted by the uploader.
8. Once all files are loaded correctly, they are displayed in the task manager.
9. Typically, this process takes a couple of minutes depending on the size of the dataset. The progress of the upload is displayed in the task manager.
10. The uploaded assets will be listed in the Assets List under the Assets tab. If not displayed, click on the Refresh button.
11. Clicking on the asset will open a pop-up window to allow you to explore the table.
12. The feature collection asset is ready to use. NB: You can visualize, share, or delete it as needed within the code editor interface.

<br>

## How to add data layers to Whisp <a name="whisp_add_data"></a>

There are two main approaches: to request a layer be incorporated into the core Whisp inputs, or to add in your own data directly to complement the core ones in Whisp

### Requesting a dataset additions
If you think a particular dataset has wide applicability for Whisp users, you can request it be added to the main Whisp repository by logging as an issue in Github [here] (https://github.com/forestdatapartnership/whisp/issues/). 

### Adding your own dataset for a bespoke Whisp analysis (using the Python Notebooks)
Adding your To add other datasets, such as that are specific to your circumstances, or can’t be shared directly in GEE, follow the steps and guidance below.

1)	Edit modules/datasets.py and add in a function to make a binary GEE image (i.e., where values are either 0 or 1*). 
2)	Choose a name to represent the data layer in the final CSV output. Place in speech brackets  in the .rename() section at the end of the function. See examples elsewhere in the functions in this script.
3)	Edit parameters/lookup_gee_datasets.csv to include the chosen dataset name in a new row. Make sure other relevant columns are filled in.
   
The above assumes a single band image that is being included, which results in a single column being added. 
If you have multiband images to add and want each band to be a layer in Whisp, make sure each band is named. 
Make sure to add all the bands to the lookup CSV (see Step 3), else they won’t appear in the output.

How to fill out the columns parameters/lookup_gee_datasets.csv
a.	dataset_id: add a unique number
b.	dataset_order: choose a number for where you want the dataset column placed in the CSV output.
c.	dataset_name: the name for the dataset column. NB must be exactly the same as the name of the image band in step 1.
d.	presence_only_flag: if 1 added here the dataset shows a value of True if it overlaps with the plot to any extent. 
e.	exclude: removes the dataset from analysis 
f.	theme: a word denoting the dataset Theme. Currently there are five themes where i to iv correspond to:
   i.	treecover: for forest or treecover at the end of 2020 
   ii.	commodities: representing commodities in 2020 (typically ones that tree cover might be confused with in remote sensing products).    
   iii.	disturbance_before: forest/tree cover disturbance before the end of 2020
   iv.	disturbance_after: forest/tree cover disturbance after the end of 2020
   v.	ancillary: other relevant layers, such as representing protected areas or areas of importance for biodiversity.
g.	use_for_risk: if 1 is added here this dataset is included in the risk calculations. The type of risk indicator it will contribute to is automatically governed by the “theme” column.
NB if there is in a 1 in the "exclude" column this over-rules all of the above and the dataset is ignored. There are functions (in the modules/risk.py), to create lists for each of the 4 indicators from the lookup csv. These are used in the "whisp_risk" function for creating default columns to include in the final overall risk column.

### Tips for preparing and adding in your data
- It’s sometimes easier to do initial checks in JavaScript and check all looks ok on the map in Code Editor, and then convert the code into Python.
- Tools that can help convert include AI interfaces such as ChatGPT, or [geemap] (https://giswqs.medium.com/15-converting-earth-engine-javascripts-to-python-code-with-just-a-few-mouse-clicks-6aa02b1268e1/).
- Check your data: Python functions will still need sense checking and putting on a map is one way to do this using functions in [geemap] (https://geemap.org/notebooks/09_plotting/)
- A binary input image is expected, but non-integer values are allowed if they range between 0 and 1. This is most appropriate for datasets that have proportion of coverage in a pixel (e.g., a value of 0.5 would represent having half the pixel covered).
- If you are adding timeseries data, when creating the function you can use loops/mapping to compile a multiband input and to name each band accordingly.

<br>

## Contributing to the Whisp code base <a name="whisp_contribute"></a>
Contributions to the Whisp code in GitHub are welcome. They can be made by forking the repository making and pushing the required changes, then making a pull request to the Whisp repository. After briefly reviewing the request, we can make a branch for which to make a new pull request to. If in doubt get in contact first or log as an issue [here] (https://github.com/forestdatapartnership/whisp/issues/).

<br>

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

**Reporting**  
Users can report violations directly to open-foris@fao.org.

