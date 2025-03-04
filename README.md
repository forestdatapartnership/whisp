whisp
=====
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/openforis/sepal/blob/master/license.txt)
[![Privacy Policy](https://img.shields.io/badge/Privacy_Policy-FAO-lightblue.svg)](https://www.fao.org/contact-us/privacy-policy-applications-use/en)
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
- [System setup](#whisp_setup)
- [Add data layers](#whisp_add_data)
- [Contribute to the code](#whisp_contribute)
- [Code of conduct](#whisp_conduct)

<br>

## Whisp pathways <a name="whisp_pathways"></a>
***Whisp*** can currently be used directly or implemented in your own code through three different pathways:


1. The Whisp App with its simple interface can be used [right here](https://whisp.openforis.org/) or called from other software by [API](https://whisp.openforis.org/documentation/api-guide). The Whisp App currently supports the processing of up to 1000 geometries per job. The original JS & Python code behind the Whisp App and API can be found [here](https://github.com/forestdatapartnership/whisp-app).

2. [Whisp in Earthmap](https://whisp.earthmap.org/?aoi=WHISP&boundary=plot1&layers=%7B%22CocoaETH%22%3A%7B%22opacity%22%3A1%7D%2C%22JRCForestMask%22%3A%7B%22opacity%22%3A1%7D%2C%22planet_rgb%22%3A%7B%22opacity%22%3A1%2C%22date%22%3A%222020-12%22%7D%7D&map=%7B%22center%22%3A%7B%22lat%22%3A7%2C%22lng%22%3A4%7D%2C%22zoom%22%3A3%2C%22mapType%22%3A%22satellite%22%7D&statisticsOpen=true) supports the visualization of geometries on actual maps with the possibility to toggle different relevant map products around tree cover, commodities and deforestation. It is practical for demonstration purposes and spot checks of single geometries but not recommended for larger datasets.

3. Datasets of any size, especially when holding more than 1000 geometries, can be "whisped" through the [python package on pip](https://pypi.org/project/openforis-whisp/). See [Colab notebook] (https://github.com/forestdatapartnership/whisp/blob/package-test-new-structure/notebooks/Colab_whisp_geojson_to_csv.ipynb) for example implementation with a geojson input. For the detailed procedure please go to the section [Whisp notebooks](#whisp_notebooks).


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

--------------------------------------------------------------------------------

![whisp convergence of proof](images/pol_story_agu.gif)


![whisp convergence of proof](images/pol_story_agu.gif)

## Run Whisp package using Python Notebooks

For most users we suggest using the Whisp App to porcess their plots.

For bespoke analyses using or implemetation in a python workflow you can sue the python package directly.

See example [Colab notebook] (https://github.com/forestdatapartnership/whisp/blob/package-test-new-structure/notebooks/Colab_whisp_geojson_to_csv.ipynb) 

# Requirements

- A Google Earth Engine (GEE) account.
- A registered cloud GEE project.
- Some experience in Python or a similar language.

More info on Whisp can be found in [here](https://openknowledge.fao.org/items/e9284dc7-4b19-4f9c-b3e1-e6c142585865)

# Python package installation

pip install openforis-whisp

...

If running the package locally we recommend using a [virtual environment](https://docs.python.org/3/library/venv.html) to keep your main python installation clean.

```
The package relies upon the earth engine api being setup correctly using a registered cloud project

# Earth Engine project name
gee_project_name="my-ee-project"
```
Where you must replace the GEE project in the 
ee.Initialize(project=gee_project_name)

Note: this should be a registered cloud project. If unsure of this check pic here: https://developers.google.com/earth-engine/cloud/assets

More info on Whisp can be found in [here](https://openknowledge.fao.org/items/e9284dc7-4b19-4f9c-b3e1-e6c142585865)


## How to add data layers to Whisp

There are two main approaches: to request a layer be incorporated into the core Whisp inputs, or to add in your own data directly to complement the core ones in Whisp

### Requesting a dataset addition
If you think a particular dataset has wide applicability for Whisp users, you can request it be added to the main Whisp repository by logging as an issue in Github [here] (https://github.com/forestdatapartnership/whisp/issues/). Before requesting consider: 1) is the resolution high enough for plot level analysis (e.g. 30m or 10m resolution), 2) is there an indication of data quality (e.g. accuracy assessment detailed in a scientific publication) and 3) is there relevant metadata available. 

### Adding your own dataset for a bespoke Whisp analysis (using the Python Notebooks)
Adding your To add other datasets, such as that are specific to your circumstances, or can’t be shared directly in GEE, follow the steps and guidance below.

1)	Edit the datasets.py and add in a function to make a binary GEE image (i.e., where values are either 0 or 1*). Make sure the function name ends with "_prep", as only functions with this suffix are used.
2)	Choose a name to represent the data layer in the final CSV output. Place in speech brackets  in the .rename() section at the end of the function. See examples elsewhere in the functions in this script.
3)	Edit parameters/lookup_gee_datasets.csv to include the chosen dataset name in a new row. Make sure other relevant columns are filled in.

The above assumes a single band image that is being included, which results in a single column being added.
If you have multiband images to add and want each band to be a layer in Whisp, make sure each band is named.
Make sure to add all the bands to the lookup CSV (see Step 3), else they won’t appear in the output.

How to fill out the columns parameters/lookup_gee_datasets.csv
a.	name: the name for the dataset column. NB must be exactly the same as the name of the image band in step 1.
b.	order: choose a number for where you want the dataset column placed in the CSV output.
c.	theme: a word denoting the dataset Theme. Currently there are five themes where i to iv correspond to:
   i.	treecover: for forest or treecover at the end of 2020
   ii.	commodities: representing commodities in 2020 (typically ones that tree cover might be confused with in remote sensing products).
   iii.	disturbance_before: forest/tree cover disturbance before the end of 2020
   iv.	disturbance_after: forest/tree cover disturbance after the end of 2020
   v.	ancillary: other relevant layers, such as representing protected areas or areas of importance for biodiversity.
d.	use_for_risk: if 1 is added here this dataset is included in the risk calculations. The type of risk indicator it will contribute to is automatically governed by the “theme” column.
NB if there is in a 1 in the "exclude_from_output" column this over-rules all of the above and the dataset is ignored. There are functions (in the modules/risk.py), to create lists for each of the 4 indicators from the lookup csv. These are used in the "whisp_risk" function for creating default columns to include in the final overall risk column.
e. exclude_from_output: removes the column from the formatted final table (to remove input code out the function in the datasets.py)
f. col_type:
    - choose 'float32' (most )
    - exceptions are 'bool' for showing True/False, where values >0 gives True.
e. is nullable: set to 1
f. is required: set to 0
g. corresponding variable: the name of the function for creating the dataset in datasets.py (should end with "_prep")


### Tips for preparing and adding in your data
•	It’s sometimes easier to do initial checks in JavaScript and check all looks ok on the map in Code Editor, and then convert the code into Python. Tools that can help convert include AI interfaces such as ChatGPT, or [geemap] (https://giswqs.medium.com/15-converting-earth-engine-javascripts-to-python-code-with-just-a-few-mouse-clicks-6aa02b1268e1/).
•	Check your data: Python functions will still need sense checking and putting on a map is one way to do this using functions in [geemap] (https://geemap.org/notebooks/09_plotting/)
•	A binary input image is expected, but non-integer values are allowed if they range between 0 and 1. This is most appropriate for datasets that have proportion of coverage in a pixel (e.g., a value of 0.5 would represent having half the pixel covered).
•	If you are adding timeseries data, when creating the function you can use loops/mapping to compile a multiband input and to name each band accordingly.

## Key files

# Parameters:
- `lookup_gee_datasets.csv` contains the list of input datasets, the order they will be displayed, which ones are to be excluded from the current analysis, and which ones are shown as flags (i.e., shown as presence or absence instead of figures).

### src code

Main Whisp analysis functions are found in the following files:
- `datasets.py` functions for compiling GEE datasets into a single multiband image ready for input into the whisp analysis
- `stats.py` functions to run Whisp analysis for each GEE dataset, providing results for coverage of each plot as an area in hectares
-`risk.py` functions for estimating risk of deforestation.

## Contributing to the Whisp code base
Contributions to the Whisp code in GitHub are welcome. They can be made by forking the repository making and pushing the required changes, then making a pull request to the Whisp repository. After briefly reviewing the request, we can make a branch for which to make a new pull request to. If in doubt get in contact first or log as an issue [here] (https://github.com/forestdatapartnership/whisp/issues/).

Install the package in editable mode with the additional dependencies required for testing and running pre-commit hooks:
```
git clone https://github.com/forestdatapartnership/whisp.git
cd whisp/
pip install -e .[dev]
```

Setup the pre-commit hooks:
```
pre-commit install
```

You should be able to run the Pytest suite by simple running the `pytest` command from the repo's root folder.

Please read the ![contributing guidelines](contributing_guidelines.md) for good practice recommendations.
