# cmip6download
This package is used to download CMIP6 data from ESGF nodes.
The data which has to be downloaded is specifed using a YAML file. Example YAML and configuration files for this module can be found in the `examples/` directory.

## Installation
1. Clone the source code from the [GitHub repository](https://github.com/maschwanden/cmip6download) and change directory into
    the cloned directory.
2. Install the module using `pip install -e .` (while you are in the src directory; there should be a `setup.py` file in it)
3. Copy the config file from `examples/config1.yaml` to `~/.config/cmip6download/config.yaml`.
4. Open the config file `~/.config/cmip6download/config.yaml` and set `base_data_dir` (this is the directory where all the CMIP6 data will be downloaded into) and `progress_logging_directory` (writable directory where logging information will be stored) accordingly.

## Update
If you have installed the module using `pip install -e .`, you can simply pull the newest version from GitHub and you should be fine (if not: uninstall the package, pull the newest version from GitHub, reinstall).

## Usage
After the module has been installed, it can be called using
```
python -m cmip6download OPTIONS QUERY_FILE
``` 
where all available arguments for OPTIONS are listed [below](#CONFIG_FILE) and QUERY_FILE is a path to a 
YAML file containing information which data should be downloaded (see [here](#QUERY_FILE)).

### Example 1

We download the file `areacello_Ofx_MIROC6_1pctCO2_r1i1p1f1_gn.nc` using `cmip6_download`.  While being in the src directory:
```
python -m cmip6download --gosearch --noverify examples/query2.yaml
```
This will download a file and store it as:
```
BASE_DATA_DIR/areacello/Ofx/1pctCO2/MIROC6/r1i1p1f1/gn/areacello_Ofx_MIROC6_1pctCO2_r1i1p1f1_gn.nc
```

where `BASE_DATA_DIR` is the directory specified in the config file (`~/.config/cmip6download/config.yaml`).

Now let's check whether `cmip6_download` finds the same data as a manual call to the CMIP6 data search interface (hereafter called DSI).

Open the DSI and manually select all the filtering options specified in the query file.
This should then result in the following outcome:
![ESGF node search result](esgf_node_search.png)

The search result is identical to what `cmip6download` has found!

## QUERY_FILE
The query file is a YAML file specifying which data should be downloaded.
A query file contains "blocks" (see below), where one block is a collection
of filtering arguments which are then used to make a single request to ESGF nodes.
Thus a block can be thought of as one call to the [CMIP6 data search interface](https://esgf-node.llnl.gov/search/cmip6/).
Each filtering argument given to a block, filters the search results.

For further information have a look at this documentation of arguments to the [ESGF CMIP6 REST API](https://rdrr.io/cran/epwshiftr/man/esgf_query.html).

Here is an example query file ([`examples/query1.yaml`](examples/quer1.yaml)):
```
#########
# Block 1
#########

- variable:
    - areacella
    - areacello
    - volcello
  frequency:
  experiment_id:
  grid_label:
  priority: 120

#########
# Block 2
#########

- variable:
    - no3
    - po4
    - co3
  frequency:
    - mon
    - yr
  experiment_id:
    - piControl
    - historical
    - ssp585
    - ssp126
  grid_label:
    - gn
  member_id:
    - r1i1p1f1
  priority: 80
```

This query file contains two "blocks".

In "Block 1" the variables "areacella", "areacello", and "volcello" are specified for downloading.
In this block no other filtering argument is provided. Thus this block will lead to the download
of all "areacella", "areacello", and "volcello" files of all scenarios, models, etc. The last
element of this block specifies the priority of this block. The priority is a arbitrary number
which specifies the relative importance of this block (and therefore the order in which blocks are processed). If no priority is given, a default value of 100 is assumed.

In "Block 2" the variables "no3", "po4", and "co3" are specified for downloading.
In contrast to the block from above more filtering arguments are provided:
- frequency: Only monthly and yearly files
- experiment_id: Only files from the scenarios "piControl", "historical", "ssp585", and "ssp126"
- grid_label: Only files on the native grid (the grid used by the ESM itself)
- member_id: Only files from members with the ID "r1i1p1f1"
Finally, the priority is set to 80.

When cmip6download is called with this query file, first the grid areas ("areacella",
"areacello", and "volcello") and in a second step (due to the lower priority number), the chemical variables ("no3", "po4", and "co3") will be downloaded.

## CONFIG_FILE
The config file specifies some global options:
- cmip6restapi_url: The base URL for the CMIP6 search REST API
- base_data_dir: The directory where the CMIP6 data which is downloaded should be stored.
- max_download_attempts: Maximum number of download approaches until a file is assumed to
be not available.
- n_worker: Number of allowed parallel downloads. If set to 10, ten files will be downloaded
in parallel.
- progress_logging_directory: Directory where logging information can be written.
- min_number_of_members: Minimum number of members. If no members are found, a new search is conducted with no members specified. From all the found members from this search the first X members are downloaded.
- max_number_of_members: Maximum number of members. If more members are found only the first X members are downloaded.

The only options in the config file which MUST BE set by the user are the `base_data_dir` and `progress_logging_directory`.

## Options
All the options are given using the "flag syntax". Thus they are provided as follows:

```
python -m cmip6download --OPTION1 --OPTION2 QUERY_FILE
```

All options except `config_file` are boolean flags (thus no values has to be provided).
The `config_file` option has to be used as follows:

```
python -m cmip6download --config_file=/path/to/config/file QUERY_FILE
```
- verify: Specifies that all the files should be verified using MD5 checksums. If some files are outdated they are re-downloaded.
- noverify: Specifies that no files should be verified.
- gosearch: Specifies that the script should directly start with downloading the files (without asking the user for confirmation again).
- debug: Activates the debug mode, which writes out more information.

If the script should NOT ask the user for any confirmation (e.g. if the script should run automatically) the options `verify`/`noverify` AND `gosearch` have to be used. If they are not both specified the script will stop until a user confirmation is received. Thus either `--verify --gosearch` or `--noverify --gosearch` must be used.
