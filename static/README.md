# Manually Curated Retired Sites

The uuid-annotator uses the siteinfo API for a list of [current site geo and
network annotations][1]. As sites are retired, they are removed from this
list. Some sites were retired before the migration to siteinfo.

[1]: https://siteinfo.mlab-oti.measurementlab.net/v1/sites/annotations.json

This directory contains a manually curated set of retired sites. The file
`retired-sites.json` uses the same format as the v1/sites/annotations.json.

The annotation-service should be able to load the current siteinfo
annotations.json file and this static `retired-sites.json` file for a
complete historical record of site geo and network annotations.

## Method

Because this list was curated manually, there may be errors. The following
outlines how the list was curated.

* Extracting retired sites from m-lab/operator/plsync

```sh
cd m-lab/operator/plsync
git log . | grep commit > commits.list
mkdir -p history
cat commits.list | while read _ commit ; do
    echo $commit
    git checkout $commit
    x=$( git show -s --format=%ct )
    # NOTE: each file includes all needed information.
    cp sites.py history/$x.py
done
```

* Extracting retired sites from m-lab/siteinfo/sites

```sh
git log . | grep commit > commits.list
mkdir -p history
cat commits.list | while read _ commit ; do
    echo $commit
    git checkout $commit
    x=$( git show -s --format=%ct )
    # NOTE: only site names are saved. So, include the commit in the filename.
    ls > history/$x-$commit.txt
done
```

* Find all historical sites and all current sites

  Use a combination of grep, awk, etc to find the complete set of historical
  sites, i.e. s1. Use the current annotations.json to list all current sites.

  Take the difference between these two groups. These are all named sites that
  are no longer current. Some can be eliminated due to:

  * test sites: *0t, *1t, etc.
  * vm sites: tyo*, *0c
  * renamed sites: [operator#278](https://github.com/m-lab/operator/pull/278)

* Determine if retired site is in plsync or siteinfo, and export

For each retired site that is in plsync, lookup the last known record. Since
the ASName and ASNumber were not part of the plsync database, lookup these
manually using information from ipinfo.io and "Copy of M-Lab Sites - Copy"
spreadsheet.

```sh
name=$1
echo $name
# ath01 83.212.4.0 2001:648:2ffc:2101:: Athens GR 37.936400 23.944400
read file site ipv4 ipv6 city country lat lon < <(
  grep $name * | grep makesite | grep .py | tail -1 | tr "'," ' ' \
      | awk '{print $1, $3, $4, $5, $6, $7, $8, $9}' )

echo $file
cat <<EOF
    {
        "Annotation": {
           "Geo": {
              "City": "$city",
              "ContinentCode": "",
              "CountryCode": "$country",
              "Latitude": $lat,
              "Longitude": $lon
           },
           "Network": {
              "ASName": "",
              "ASNumber": 0,
              "Systems": [
                 {
                    "ASNs": [
                        0
                    ]
                 }
              ]
           },
           "Site": "$site"
        },
        "Name": "$site",
        "Network": {
           "IPv4": "$ipv4/26",
           "IPv6": "$ipv6/64"
        }
    }
EOF
```

For each retired site in siteinfo, checkout the last commit, and reference
the site in a sample jsonnet template like the one below:

```jsonnet
local site = import 'sites/yqm02.jsonnet';
{
  Annotation: {
    Geo: {
      City: site.location.city,
      ContinentCode: site.location.continent_code,
      CountryCode: site.location.country_code,
      Latitude: site.location.latitude,
      Longitude: site.location.longitude,
      State: site.location.state,
    },
    Network: {
      ASName: site.transit.provider,
      ASNumber: site.transit.asn,
      Systems: [
        {
          ASNs: [
            site.transit.asn,
          ],
        },
      ],
    },
    Site: site.name,
  },
  Name: site.name,
  Network: {
    IPv4: site.network.ipv4.prefix,
    IPv6: site.network.ipv6.prefix,
  },
}
```

And run `jsonnet` to export the values. Double check the values.

```sh
jsonnet -J . ./retired.jsonnet
```
