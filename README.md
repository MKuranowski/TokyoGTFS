TokyoGTFS
==========

Description
-----------

Creates GTFS and feed for Tokyo from various data sourced, mainly the 
[Open Data Challenge for Public Transportation in Tokyo](https://tokyochallenge.odpt.org/)
and the [Public Transportation Open Data Center](https://developer.odpt.org/).



Precautions
-----------

You need to have both the Tokyo Challenge apikey and ODPT apikey.

Create a file called `apikeys.json` with the following content:

```json
{
    "odpt": "YOUR_ODPT_APIKEY_HERE",
    "odpt_tokyo": "YOUR_TOKYO_CHALLENGE_APIKEY_HERE"
}
```

Alternatively, the script can read the apikeys from `APIKEY_ODPT` and `APIKEY_ODPT_TOKYO`
environment variables.

It's also possible to put the apikeys.json file in a different place - if path to that
file is provided via the `APIKEYS_FILE` environment variable.


Running
-------

TokyoGTFS is written in [Python 3](https://python.org) and depends on several external modules:
- [Requests](http://docs.python-requests.org/en/master/),
- [ijson](https://pypi.org/project/ijson/),
- [pytz](https://pythonhosted.org/pytz/).

Before launching install those using `pip3 install -r requirements.txt`, or e.g. from your package manager.

Currently, only a script for converting static rail data is available.

Here's a list of all the scripts.

### static.rail

`python3 -m static.rail`

Options: `-h`/`--help`, `-v`/`--verbose`.

After those flags, one has to specify what action to take:

- **create-gtfs** (`python -m static.rail create-gtfs`):  
  Creates the GTFS file. See `python -m static.rail create-gtfs --help` for all the options.

- **dump-provider** (`python -m static.rail dump-provider <provider_name>`):  
  Dumps all train of a provider into `data_cached` (which can be later re-used in `create-gtfs --from-cache`).  
  Used to debug problems with a specific provider.

- **check-geo** (`python -m static.rail check-geo [agency-or-line]`):  
  Used internally to check and verify the structure of the curated rail_geo.osm.

- **check-names** (`python -m static.rail check-names`):  
  Used internally to check the structure of the curated station_names.csv file.


Attributions
------------
Always use the data in compliance with sources' terms and conditions.
See <attributions.md> for a list of all used sources.


License
-------

TokyoGTFS is shared under the [MIT License](https://github.com/MKuranowski/TokyoGTFS/blob/master/LICENSE.md) license, included in the file *license.md*.