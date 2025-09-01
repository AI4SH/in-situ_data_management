# Organise AI4SoilHealth in-situ data

Created by Thomas Gumbricht, Stockholm University for the EU funded project [Ai4SoilHealth](https://ai4soilhealth.eu).

Last edits: 26 August 2025

## SUMMARY

This repository consists of a Jupyter notebook front calling a set of Python packages for converting Comma Separated Values (CSV) files to structured json files. This README.md file explains how to use the Jupyter notebook, the Python packages are documented [here](https://xspectrometrics.github.io/ai4sh_in-situ_data_management/import_csv_data/docs/html/).

## Objective

The Jupyter notebook  [AI4SH in-situ data management](https://github.com/xspectrometrics/ai4sh_in-situ_data_management/blob/main/README.md) compiles in-situ data from soil sampling campaigns into organised json documents. The input data must be organised as CSV files, typically created by first putting the in-situ sampled data together in a spreadsheet and then exporting the spreadsheet as a .csv file.

## Input data requirements and organisation

To run the notebook at least four files must be defined, i) one defining the user project settings (_user project file_), ii) one defining process parameters (_process file_), iii) one with the actual data (_data source file_), and iv) one that defines the columns (header) in the data file (_method source file_). The latter two are defined as process parameters in the _process file_, whereas the _user project file_ and the _process file_ are defined directly in the notebook.  

### User project file

The user project file must be in json format and defines the path to the root folder where all the other files can be found. The user project file also defines the user and if database access is required (not in the present version of the notebook) the login credentials for the given user must be given. Running the notebook without a database connection, the _host_, _db_ and _host_netrc_id_ values should be set to _null_.

```
{
  "project_path": "/Path/To/projects/ai4sh_sueloanalys/digit-soil_SEAR",
  "postgresdb": {
  	"host": null,
    "db": null,
    "host_netrc_id": null
  },
  "userproject": {
    "userid": "thomasg",
    "user_netrc_id": "thomasg_xspectre",
    "user_pswd": ""
  },
  "process": [
    {
      "verbose": 3,
      "overwrite": false,
      "delete": false,
      "src_path": {
        "volume": ".",
        "ext": "json"
      }
    }
  ]
}
```

### Process file

A _process_file_, whether linked directly in the notebook or via a _job file_ (see below) must include the _sub_process_id_ (at present the only defined _sub_process_id_ is _import_csv_single-lines_) and the full paths to the _data source file_ (_data_src_FPN_), the _method source file_ (_method_src_FPN_) and the path to the destination directory (_dst_FP_) where to save the json formatted output.

If there are metadata entries that are exactly identical for all records (lines) in the _data source file_ (e.g. the person responsible for the field sampling, the analysis or the sample logistics), then this metadata can be stated in the _process file_ instead of being repeated for evay record (line) in the _data source file_, as in the example below.

```
{
  "process": [
    {
      "overwrite": false,
      "sub_process_id": "import_csv_single-lines",
      "parameters": {
        "user_email_sampling": "sampling@gmail.com",
        "user_email_logistic": "logistic@gmail.com",
        "user_email_analysis": "analysis@gmail.com",
        "method_src_FPN": "/Users/thomasgumbricht/projects/ai4sh_sueloanalys/header_dictionary/header_dictionary.csv",
        "data_src_FPN": "/Users/thomasgumbricht/projects/ai4sh_sueloanalys/digit-soil_SEAR/csv_data/AI4SH_SEAR_all_results.csv",
        "dst_FP": "/Users/thomasgumbricht/projects/ai4sh_sueloanalys/digit-soil_SEAR/database"
      }
    }
  ]
}

```

You can add any number of processes in the array _process_ in the _process file_.

```
{
  "process": [
    {
      "overwrite": false,
      "sub_process_id": "import_csv_single-lines",
      "parameters": {
        ...
        ...
      }
    },
    {
      "overwrite": false,
      "sub_process_id": "import_csv_single-lines",
      "parameters": {
        ...
        ...
      }
    }
  ]
}

```

If there are relatively few data files to process, the best option is to put these few processes in a single _process file_ and link that _process file_ directly from the notebook.

### Job file

If you have a multitude of files to process, or if you need to set varying common records (e.g. different persons responsible for the field sampling, the analysis or the sample logistics), an alternative is to use a _job file_ that links to a set of sequential _process files_.

In a _job file_ you can link to a sequence of _process files_ either as an array directly in the _job file_ or by linking to a _pilot file_ (simple text file) that lists the _process files_ to execute.

List the _process files_ to run as an array directly in the _job file_.
```
{
  "process": {
    "job_folder": "doc",
    "pilot_list": [
      "process_digit-soil_sear.json",
      "process_microbiometer.json"
    ],
    "process_sub_folder": "json"
  }
}
```

Or link to a _pilot_file in the _job_file_:
```
{
  "process": {
    "job_folder": "doc",
    "pilot_file": "ai4sh_import_csv_data.txt",
    "process_sub_folder": "json"
  }
}
```

The json file objects _job_folder_ (_doc_ in the example above) and _process_sub_folder_ (_json_) in the example above, refer to the hierarchical path structure of the files to process. For the example above the strucure that is expected is this:

```
.
├── ai4sh_import_digit-soil_sear.json
└── doc
    ├── ai4sh_import_csv_data.txt
    └── json
        └── process_digit-soil_sear.json
```

Where the root directory (denoted by ".") is the _project_path_ given in the _user_project_file_.

### Pilot file

A _pilot file_ is a simple text files that lists the _process_files_ to run and where blank lines or lines starting with hashtag are ignored:

```
#### All digit soil SEAR in one process ####

process_digit-soil_sear.json
```

### Data source file

All the actual data from the in-situ sampled soil analysis must be in the _data source file_. In addition, all metadata that are unique to each sample (e.g the sample point id) must also be listed in the data source file. The colums headers in the data source file can, in principle, have any name, the actual content transferred to the json output files is defined in the _method source file_. To make life easier it is, however, better to set the header column in the _data source file_ to the parameter name used in the json output.

Required metadata to generate the json output include:
- pilot_country,
- pilot_site,
- sample_id,
- min_depth,
- max_depth,
- sample_date,
- sample_analysis_date,
- user_email_sampling,
- user_email_logistic, and
- user_email_analysis.

Optional metadata, assigned default values (in parenthesis) if missing in the _data source file_, include:

- subsample (a),
- replicate (0),
- sample_preparation__name (None),
- sample_preservation__name (None),
- sample_transport__name (None),
- sample_storage__name (None),
- transport_duration_h (0), and
- comment ().

Metadata that is common across all samples listed in a _data source file_ can instead be added as parameters in the _process_file_, and typically include:
- user_email_sampling,
- user_email_logistic, and
- user_email_analysis.

But also other metadata can be put in the _process file_ instead of the _data source file_ If your _data soruce file_ contains data only for a single site (or pilot_site) you can omit the parameters for _pilot_country_ and _pilot_site_ from the _data source file_ and add them as parameteres in the _process_file_. Similarly, if all samples are taken the same date you can also put the _sample_date_ parameter in the _process_file_ and omit it from the _data source file_.

As noted in the very beginning of this paragraf, the actual data on soil properties **must** be listed in the _data source file_. As the actual data can be in many different units, derived from a variety of methods and meaasured with different instruments (inlcuding different makes and models), the parameters to include in the _data source file_ are not predefiend. But they must be defined in the _method source file_; defined regarding three properties:
- method,
- equipment, and
- unit.

Example of _data source file_:

| pilot_country | pilot_site | sample_id | min_depth | max_depth | Sample_date | Sample_analysis_date | data_1_id | data_2_id |
| -------- | ------- | -------- | ------- | -------- | ------- | -------- | ------- | ------- |
| SE | some_place | x | 0 | 20 | 20240815 | 20240816 | x.y | m.n |
| SE | some_place | x | 20 | 50 | 20240815 | 20240816 | a.b | i.j |
| SE | some_place | y | 0 | 20 | 20240815 | 20240816 | c.d | e.f |

### Method source file

A method source file, in contrast to the _data source file_, must have exactly 6 columns, all with the exact headers:

- "header",
- "parameter",
- "unit",
- "method",
- "equipment", and
- "url".

Entries in the _header_ column must correspond to column headers (exact spelling) in the _data source file_. You might thus edit the records in the _header_ column in the _method source file_. The second column in the _method source file_, _parameter_, is the object name that will appear in the json output file. It should not be changed. The columns for _unit_, _method_ and _equipment_ should only be filled for actual records (lines) where the _header_ column corresponds to data. For metadata these fields should all be set to "None".

The method file corresponding to the example _data source file_ above should thus contain information on the observed data (_data_1_id_ and _data_2_id_):


| header | parameter| unit | method | equipment | url |
| -------- | ------- | -------- | ------- | -------- | ------- |
| pilot_country | pilot_coutry | None | None | None | None |
| sample_id | sample_id | None | None | None | None |
| min_depth | min_depth | None | None | None | None |
| max_depth | max_depth | None | None | None | None |
| min_depth | min_depth | None | None | None | None |
| sample_date | sample_date | None | None | None | None |
| sample_analysis_date | sample_analysis_date | None | None | None | None |
| data_1_id | data_1 | data_unit | analysis_method | analysis_equipment | analysis_url |
| data_1_id | data_1 | data_unit | analysis_method | analysis_equipment | analysis_url |
