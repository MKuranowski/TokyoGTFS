TokyoGTFS
=========

Create GTFS data for Tokyo trains and buses.

Written in [Python](https://www.python.org/) and with the [Impuls framework](https://impuls.readthedocs.io).


Running
-------

1. Make sure you have a recent Python installation
2. Create a new [virtual environment](https://docs.python.org/3/library/venv.html) for dependency management: `python -m venv --upgrade-deps .venv`
3. Activate the venv: `. .venv/bin/activate`
4. Install dependencies: `pip install -Ur requirements.txt`
5. [Provide API keys](#api-keys)
6. Run the desired script, e.g. `python -m tokyo_gtfs.rail`


Railway
-------

The railway GTFS contains schedule and through-service (block_id) data for all urban railways
in the greater metropolitan area. The data is made available by the excellent
[mini-tokyo-3d project](https://github.com/nagix/mini-tokyo-3d/), with some minor tweaks applied.

Note that through-running occurs between multiple route types; that is a single block_id
might join both rail and metro trips. This is intentional and must be supported by the consuming
software.

Beware that the data is licensed under the [MIT License](https://github.com/nagix/mini-tokyo-3d/blob/master/LICENSE),
and with the following copyright: `Copyright (c) 2019-2025 Akihiko Kusanagi`.


Bus
---

Most operators in the greater Tokyo area already share a GTFS and a GTFS-Realtime feeds,
see <https://ckan.odpt.org/dataset/?tags=%E3%83%90%E3%82%B9-bus&res_format=GTFS%2FGTFS-JP>.

The script only creates GTFS for operators without GTFS feeds, but with ODPT JSON data; that is:

- Tobu Bus,
- Kanachu,
- Kokusai Kogyo Bus,
- Tokyu Bus,
- Odakyu Bus,
- Sotetsu Bus.

The data for those operators is very bare-bone, and thus the resulting GTFS is also quite bare-bone.
None of these operators share realtime data.


API Keys
--------

As of now, API keys are only required for creating the bus GTFS. One must register at <https://developer.odpt.org/>
and obtain the "standard"/ODPT center apikey and the "challenge" apikey. Those keys must then be set
in the `TOKYO_ODPT_APIKEY` and `TOKYO_CHALLENGE_APIKEY` environment variables, e.g.:

```terminal
$ export TOKYO_ODPT_APIKEY=apikey1
$ export TOKYO_CHALLENGE_APIKEY=apikey2
$ python -m tokyo_gtfs.bus
```

Most IDEs will have some support for .env files – use them to avoid having to export environment
variables when running the scripts.

As an alternative, TokyoGTFS also accepts Docker-style secret passing. Instead of setting
`TOKYO_ODPT_APIKEY` or `TOKYO_CHALLENGE_APIKEY` directly, the apikeys can be saved in files,
and paths to those files can be set through the `TOKYO_ODPT_APIKEY_FILE` or
`TOKYO_CHALLENGE_APIKEY_FILE` environment variables. Beware that the direct/non-file env variables
take precedence.
