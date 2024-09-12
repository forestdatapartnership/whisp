Source list behind lookup_gee_datasets.csv

<b>Tree cover datasets:</b>
<table>
  <thead>
    <tr>
      <th style="width: 150px;">Dataset name</th>
      <th style="width: 250px;">Description of zonal statistics applied</th>
      <th style="width: 500px;">Source and GEE asset</th>
    </tr>
  </thead>
  <tbody>
    <tr><td colspan="3"><b>Tree cover datasets:</b></td></tr>
    <tr><td rowspan="3">EUFO_2020</td><td rowspan="3">Binary values, where 1 is forest.</td><td rowspan="2">Bourgoin, C., Ameztoy, I., Verhegghen, A., Carboni, S., Colditz, R.R., Achard, F. (2023). <i>Global map of forest cover 2020</i> (Version 1). European Commission, Joint Research Centre (JRC) [Dataset] PID: http://data.europa.eu/89h/10d1b337-b7d1-4938-a048-686c8185b290.</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“JRC/GFC2020/V1”)</td></tr>
    <tr><td rowspan="3">GLAD_Primary</td><td rowspan="3">Binary input layer representing primary forest in 2001. Loss pixels 2001-2020 removed with ancillary GFC dataset.</td><td rowspan="2">Turubanova, S., Potapov, P. V., Tyukavina, A., & Hansen, M. C. (2018). <i>Ongoing primary forest loss in Brazil, Democratic Republic of the Congo, and Indonesia.</i> Environmental Research Letters, 13(7), 074028. https://doi.org/10.1088/1748-9326/aacd1c</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection (‘UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1’)</td></tr>
    <tr><td rowspan="3">TMF_undist</td><td rowspan="3">Select tropical moist forest classes (10, 11 & 12) in 2020 representing undisturbed cover.</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10), eabe1603. https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘projects/JRC/TMF/TransitionMap_Subtypes’)</td></tr>
    <tr><td rowspan="3">GFC_TC_2020</td><td rowspan="3">Select areas of tree cover over 10 percent and remove loss pixels between.</td><td rowspan="2">Hansen, M. C., Potapov, P. V., Moore, R., Hancher, M., Turubanova, S. A., Tyukavina, A., Thau, D., Stehman, S. V., Goetz, S. J., Loveland, T. R., Kommareddy, A., Egorov, A., Chini, L., Justice, C. O., & Townshend, J. R. G. (2013). <i>High-Resolution Global Maps of 21st-Century Forest Cover Change.</i> Science, 342(6160), 850–853. https://doi.org/10.1126/science.1244693. Data available online from: https://glad.earthengine.app/view/global-forest-change.</td></tr>
    <tr></tr>
    <tr><td>ee.Image(“UMD/hansen/global_forest_change_2022_v1_10”)
</td></tr>
    <tr><td rowspan="3">JAXA_FNF_2020</td><td rowspan="3">Select dense and non-dense forest classes (i.e., classes 1 and 2), and data from the year 2020.</td><td rowspan="2">Shimada, M., Itoh, T., Motooka, T., Watanabe, M., Shiraishi, T., Thapa, R., & Lucas, R. (2014). <i>New global forest/non-forest maps from ALOS PALSAR data (2007–2010).</i> Remote Sensing of Environment, 155, 13–31. https://doi.org/10.1016/j.rse.2014.04.014</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘JAXA/ALOS/PALSAR/YEARLY/FNF4’)</td></tr>
    <tr><td rowspan="3">GLAD_LULC_2020</td><td rowspan="3">Select classes from the 2020 land cover map representing stable tree cover over 5m in height (i.e. classes 27 to 48, and 127 to 148).</td><td rowspan="2">Potapov, P., Hansen, M. C., Pickens, A., Hernandez-Serna, A., Tyukavina, A., Turubanova, S., Zalles, V., Li, X., Khan, A., Stolle, F., Harris, N., Song, X.-P., Baggett, A., Kommareddy, I., & Kommareddy, A. (2022). <i>The Global 2000-2020 Land Cover and Land Use Change Dataset Derived From the Landsat Archive: First Results.</i> Frontiers in Remote Sensing, 3, 856903. https://doi.org/10.3389/frsen.2022.856903</td></tr>
    <tr></tr>
    <tr><td>ee.Image(‘projects/glad/GLCLU2020/v2/LCLUC_2020’)</td></tr>
    <tr><td rowspan="3">ESA_TC_2020</td><td rowspan="3">Select trees and mangrove classes (i.e., 10 and 95) for 2020.</td><td rowspan="2">Zanaga, D., Van De Kerchove, R., De Keersmaecker, W., Souverijns, N., Brockmann, C., Quast, R., Wevers, J., Grosu, A., Paccini, A., Vergnaud, S., Cartus, O., Santoro, M., Fritz, S., Georgieva, I., Lesiv, M., Carter, S., Herold, M., Li, L., Tsendbazar, N.-E., Ramoino, F., Arino, O. (2021). <i>ESA WorldCover 10 m 2020 v100</i> ( v100) [Dataset]. Zenodo. https://doi.org/10.5281/ZENODO.5571936</td></tr>
    <tr></tr>
    <tr><td>ee.Image(“ESA/WorldCover/v100/2020”)</td></tr>
    <tr><td rowspan="3">ESRI_TC_2020</td><td rowspan="3">Select tree class (i.e., 2) for 2020.</td><td rowspan="2">Karra, K., Kontgis, C., Statman-Weil, Z., Mazzariello, J. C., Mathis, M., & Brumby, S. P. (2021). <i>Global land use / land cover with Sentinel 2 and deep learning.</i> 2021 IEEE International Geoscience and Remote Sensing Symposium IGARSS, 4704–4707. https://doi.org/10.1109/IGARSS47720.2021.9553499</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS")</td></tr>
    <tr><td colspan="3"><b>Commodity datasets:</b></td></tr>
    <tr><td rowspan="3">TMF_plant</td><td rowspan="3">Select classes representing any type of plantation (i.e., classes 81-86).</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10), eabe1603. https://doi.org/10.1126/sciadv.abe1603, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10), eabe1603. https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘projects/JRC/TMF/v1_2021/TransitionMap_Subtypes’)</td></tr>
    <tr><td rowspan="3">Oil_palm_Descals</td><td rowspan="3">Selected classes from the “classification band” representing oil palm plantations( i.e. classes 0 and 1).</td><td rowspan="2">Descals, A., Wich, S., Meijaard, E., Gaveau, D. L. A., Peedell, S., & Szantoi, Z. (2021). <i>High-resolution global map of smallholder and industrial closed-canopy oil palm plantations.</i> Earth System Science Data, 13(3), 1211–1231. https://doi.org/10.5194/essd-13-1211-2021</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘BIOPAMA/GlobalOilPalm/v1’)</td></tr>
    <tr><td rowspan="3">Oil_palm_FDaP</td><td rowspan="3">Binary layer. For select countries only.</td><td rowspan="2">FDaP. 2024. Lookup GEE datasets. In: Forest Data Partnership – GitHub. [Cited 17 January 2024]. https://github.com/forestdatapartnership/whisp/blob/main/parameters/lookup_gee_datasets.csv</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“projects/forestdatapartnership/assets/palm/palm_2020_model_20231026”)</td></tr>
    <tr><td rowspan="3">Cocoa_ETH</td><td rowspan="3">Binary product where 1 represents cocoa. Product derived from a cocoa probability map where the recommended threshold of >65%, had already been applied.</td><td rowspan="2">Kalischek, N., Lang, N., Renier, C., Daudt, R. C., Addoah, T., Thompson, W., Blaser-Hart, W. J., Garrett, R., Schindler, K., & Wegner, J. D. (2022). <i>Satellite-based high-resolution maps of cocoa planted area for Côte d’Ivoire and Ghana</i> ( 5). arXiv. https://doi.org/10.48550/ARXIV.2206.06119</td></tr>
    <tr></tr>
    <tr><td>ee.Image(‘projects/ee-nk-cocoa/assets/cocoa_map_threshold_065’)</td></tr>
    <tr><td rowspan="3">Cocoa_bnetd</td><td rowspan="3">Select commodity classes. For Côte d'Ivoire only.</td><td rowspan="2">BNETD (2024). <i>Occupation des sols de la Côte d'Ivoire en 2020</i> (Version 2).
Centre d'Information Géographique et du Numérique / Bureau National d’Études Techniques et de Developpement. Côte d'Ivoire, 2024. Data available online from: https://arcg.is/0uHOi90</td></tr>
    <tr></tr>
    <tr><td>ee.Image("projects/ee-bnetdcign2/assets/OCS_CI_2020vf")</td></tr>
    <tr><td colspan="3"><b>Disturbance before & after 2020 datasets:</b></td></tr>
    <tr><td rowspan="3">TMF_deg_before_2020 <br> TMF_def_before_2020 <br> TMF_deg_after_2020 <br> TMF_def_after_2020</td><td rowspan="3">Select classes representing tree cover classifications (i.e., classes 1 and 2).</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10), eabe1603. https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘projects/JRC/TMF/TransitionMap_Subtypes’)</td></tr>
    <tr><td rowspan="3">GFC_loss_before_2020 <br> GFC_loss_after_2020</td><td rowspan="3">Select loss of the years in question.</td><td rowspan="2">Hansen, M. C., Potapov, P. V., Moore, R., Hancher, M., , S. A., Tyukavina, A., Thau, D., Stehman, S. V., Goetz, S. J., Loveland, T. R., Kommareddy, A., Egorov, A., Chini, L., Justice, C. O., & Townshend, J. R. G. (2013). <i>High-Resolution Global Maps of 21st-Century Forest Cover Change.</i> Science, 342(6160), 850–853. https://doi.org/10.1126/science.1244693. Data available online from: https://glad.earthengine.app/view/global-forest-change.</td></tr>
    <tr></tr>
    <tr><td>ee.Image(“UMD/hansen/global_forest_change_2022_v1_10”)</td></tr>
    <tr><td rowspan="3">RADD_before_2020 <br> RADD_after_2020</td><td rowspan="3">Select confirmed (i.e., class 3) alerts since year in question.</td><td rowspan="2">Reiche, J., Mullissa, A., Slagter, B., Gou, Y., Tsendbazar, N.-E., Odongo-Braun, C., Vollrath, A., Weisse, M. J., Stolle, F., Pickens, A., Donchyts, G., Clinton, N., Gorelick, N., & Herold, M. (2021). <i>Forest disturbance alerts for the Congo Basin using Sentinel-1.</i> Environmental Research Letters, 16(2), 024005. https://doi.org/10.1088/1748-9326/abd0a8</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“projects/GLAD/alert/RADD”) <br>ee.ImageCollection(‘UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1’)</td></tr>
    <tr><td rowspan="3">MODIS_fire_before_2020 <br> MODIS_fire_after_2020</td><td rowspan="3">Aggregate of burnt areas of the years in question.</td><td rowspan="2">Giglio, L., Justice, C., Boschetti, L., & Roy, D. (2021). <i>MODIS/Terra+Aqua Burned Area Monthly L3 Global 500m SIN Grid V061</i> [Dataset]. NASA EOSDIS Land Processes Distributed Active Archive Center. https://doi.org/10.5067/MODIS/MCD64A1.061</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("MODIS/061/MCD64A1")</td></tr>
    <tr><td rowspan="3">ESA_fire_before_2020</td><td rowspan="3">Aggregate of burnt areas between 2001 and 2020.</td><td rowspan="2">Lizundia-Loiola, J., Otón, G., Ramo, R., & Chuvieco, E. (2020). <i>A spatio-temporal active-fire clustering approach for global burned area mapping at 250 m from MODIS data.</i> Remote Sensing of Environment, 236, 111493. https://doi.org/10.1016/j.rse.2019.111493</a></td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("ESA/CCI/FireCCI/5_1")</td></tr>
  </tbody>
</table>


<br><br><br>


**Ancillary datasets:**
| Dataset name | Description of zonal statistics applied | Citation |
| -- | -- | -- |
| WDPA | -- | -- |
| OECM | -- | -- |
| KBA | -- | -- |
