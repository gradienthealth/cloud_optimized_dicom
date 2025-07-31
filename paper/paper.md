---
title: 'Cloud Optimized Dicom: A Python package for efficiently ingesting & manipulating medical imagery in cloud storage'
tags:
  - Python
  - dicom
  - google cloud
  - COD
  - optimization
authors:
  - name: Cal J. Nightingale
    orcid: 0009-0005-5711-1793
    affiliation: 1
  - name: Ouwen Huang
    affiliation: 1
affiliations:
 - name: Gradient Health, United States
   index: 1
date: 31 July 2025
bibliography: paper.bib
---

# Summary

The Cloud Optimized Dicom package (or COD) provides a framewowrk for storing, manipulating,
and retriving dicom files in the cloud in a cost-optimal way.
We propose a novel data structure for storing dicom data at scale, consisting of the following series-level files:
- {series_uid}.tar: contains all instance.dcm files for this series
- {series_uid}/metadata.json: contains all dicom tags for each instance, along with additional metadata
- {series_uid}/index.sqlite: an index used by the `Ratarmount` package [@ratarmount] 
to efficiently retrieve individual instances from the tar without indexing the whole thing
- (OPTIONAL) {series_uid}/thumbnail.{mp4|jpg}: a 100x100px thumbnail containing each frame in the series
These files are located at a URI conformant to the DICOMWEB spec, e.g. "{my-bucket}/studies/{study_uid}/series/{series_uid}.tar".
Fetching and caching of the series tar is abstracted away from the end user in an optimal manner.
Additional utility functionality is also included, such as the ability to add custom metadata fields, generate thumbnails, 
and use a user-provided hash function to de-identify the UIDs in the URI and metadata.

# Statement of need

At Gradient Health, we store over 5 petabytes dicom data and counting. 
Specifically, we have over 18M studies, broken into 67M series and ???M (TODO: estimate) or more instances.
Because GCP and other cloud providers bill by the GET request, if we stored this data as raw dicom at the instance level,
it would cost us (TODO: some math to show a big number here) to retrieve our entire dataset.
We, along with other data providers/hospitals/etc. who store large quanitites of DICOM data, 
were in need of some way to reduce this cost.

With COD, we were able to reduce our montly cloud storage costs by __% (TODO: estimate or compute).

AI systems need high throughput table scans of data. The system is gonna go thru every singel data point when you're training. If your files are all sharded, it will cost you a TON.

(clearly show why COD is a good thing)
1. transition costs for storage - if you store a bunch of tiny files it is quite expensive. To transition them would cost more than the storage. BREAKDOWN (for example w CT images), concrete numbers
2. minimal data corruption - why not just convert to multiframes? answer: bc multiframes can have errors, hospitals themselves can provide corrupt data... the less you touch it, the better the data providence.
TRADEOFF: we do have heavier writes, BUT dicom data is not really being updated that much (or shouldnt be). Main use case is retrieval.

add a figure explaining the format?

idea of extending cod w/ additional data (like thumbnails)

cite: proprietary implementations like intelerad (shards the data) - identify limitation of data corruption risk due to manipulation.
cite: google cloud for healthcare/amazon as prop. implementation - identify limitation of high cost. What's the compute cost to bytes processed metric?

Cost of COD: compute cost to run it (get numbers for this - bytes processed metric), and after there's the cost of transitioning storage (standard -> archive), and finally the actual storage cost.

Show COD is vastly cheaper as an option when it comes to the alts like amazon/google.

Company that does something similar: juiceFS: https://juicefs.com/en/. What COD is but for general data. Refernece it and say the idea of using cloud buckets for AI training is the up and coming way. Why do that? its because they scale. You can have petabytes of cloud stored data but you're not gonna have SSDs at the petabyte level for a single GPU cluster. This is an idea that has been gaining traction. Why not just use JuiceFS? Its about having a simple to use and export dicom. Dicom already has the P10 format for moving things around, we basically keep the P10 format and it only requies the data to be un-tarred which is a very common interface (would never require driver install or custom code handling for example.)

# Benchmarks
demonstrate cost of COD conversion on the following datasets:
- laplace-open-embed (emory)
- nih chest xrays: https://cloud.google.com/healthcare-api/docs/resources/public-datasets/nih-chest
- nlst cancer: https://cdas.cancer.gov/nlst/

show cost of: cod ingetstion + thumbnail gen, storage cost post cod.
TABLE SCAN COST: frames perspective = 4B frames, series = 55M series for example. 
Include storage cost pre cod?

# Future Directions
- data loader (ARPA-H?). if you want to actually train AI on COD data, it would suck to take COD and reformat it to another format that training can actually use. Showcase a pytorch wrapper that is able to laod the data and use it very quickly (high throughput)
- support additional cloud providers
- pixel sharding?

# Acknowledgements

TODO

# References

TODO